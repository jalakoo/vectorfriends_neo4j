from dataclasses import dataclass
from basicauth import decode
from neo4j import GraphDatabase, basic_auth
import functions_framework
import os
from openai import OpenAI
import json

# Note: Pydantic does not appear to work properly in Google Cloud Functions

HOST = os.environ.get("NEO4J_URI")
PASSWORD = os.environ.get("NEO4J_PASSWORD")
USER = os.environ.get("NEO4J_USER", "neo4j")
DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")
OPENAI_KEY = os.environ.get("OPENAI_API_KEY")


@dataclass(frozen=True)
class FormData:
    email: str
    firstName: str
    techStack: str
    openSource: str
    learnTech: str
    tenant: str


@dataclass(frozen=True)
class OpenSource:
    name: str
    description: str


CLIENT = OpenAI(
    api_key=OPENAI_KEY,
)


def extract_name_description(sentence: str) -> str:

    prompt = f'Return a JSON dictionary with the keys "name" and "description" from a user statement about their favorite Open Source project. For example, return {{"name":"TensorFlow", "description":"An end-to-end open source platform for machine learning"}} from the sentence: "My favorite open source project is TensorFlow. It is an end-to-end open source platform for machine learning."'

    response = CLIENT.chat.completions.create(
        model="gpt-3.5-turbo-1106",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": sentence},
        ],
        max_tokens=200,
        n=1,
        stop=None,
        temperature=0.0,
    )

    extracted = response.choices[0].message.content
    extracted = json.loads(extracted)

    if not isinstance(extracted, dict):
        raise ValueError("Expected dictionary was not returned from the LLM")

    try:
        extracted = OpenSource(**extracted)
    except Exception as e:
        raise ValueError(
            f"Expected dictionary keys not returned from the LLM. Recieved: {extracted}"
        )

    print(f"Opensource data extracted: {extracted}")

    return extracted


def extract_topics(sentence: str) -> list[str]:

    prompt = f'Return a JSON List of any application names, software technologies, or programming languages from a user statement. For example, return ["NextJS", "Django", "PostgresSQL"] from the sentence "NextJS + Django + PostgreSQL" or the sentence "I am most comfortable with NextJS, Django, and PostgresSQL". Always return only a single, combined list of answers. If none are found, simply return an empty list "[]"'

    response = CLIENT.chat.completions.create(
        model="gpt-3.5-turbo-1106",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": sentence},
        ],
        max_tokens=200,
        n=1,
        stop=None,
        temperature=0.0,
    )

    extracted = response.choices[0].message.content

    print(f"Response from LLM: {extracted}")

    extracted = json.loads(extracted)

    # LLM may return a dictionary instead of the requested list
    if isinstance(extracted, dict):
        aggregate = []
        # LLM may return several lists within a dictionary.
        for key in list(extracted.keys()):
            a_list = extracted[key]

            # Skip any key-value payloads that aren't a list of strings
            if not isinstance(a_list, list) and not all(
                isinstance(item, str) for item in a_list
            ):
                print(
                    f"Skipping dictionary payload '{a_list}' that is not entirely a list of strings."
                )
                continue

            # Aggregate all the results
            aggregate.extend(a_list)

        extracted = aggregate

    if not isinstance(extracted, list) or not all(
        isinstance(item, str) for item in extracted
    ):
        raise ValueError(
            f"Expected list of strings was not returned from the LLM: {extracted}"
        )

    print(f"Topics extracted: {extracted}")

    return extracted


def upload_to_neo4j(query, params):
    try:
        with GraphDatabase.driver(
            HOST, auth=basic_auth(USER, PASSWORD), database=DATABASE
        ) as driver:
            records, _, _ = driver.execute_query(query, params)
            return records
    except Exception as e:
        print(f"Upload query error: {e}")
        return None


def ingest_form(form: FormData):

    print(f"FormData received: {form}")

    # Create a user node w/ email
    # Create a tenant node
    # Create a user-tenant relationship
    # Extract topics from techStack
    # Create a tech node for each topic
    # Create a tech-user relationship
    # Extract topics from learnTech
    # Create a tech node for each topic
    # Create a tech-user relationship
    # Create a openSource node
    # Create a openSource-user relationship

    knows_tech = []
    interested_tech = []
    opensource = []
    all_tech = set()

    try:
        knows_tech = extract_topics(form.techStack)
    except Exception as e:
        print(
            f"Error parsing techStack from form:{form}: {e}. Skipping techStack ingestion..."
        )

    try:
        interested_tech = extract_topics(form.learnTech)
    except Exception as e:
        print(
            f"Error parsing learnTech from form:{form}: {e}. Skipping learnTech ingestion..."
        )

    try:
        opensource = extract_topics(form.openSource)
    except Exception as e:
        print(
            f"Error parsing openSource tech from form:{form}: {e}. Skipping openSource ingestion..."
        )

    # Create User and Tenant records
    query_0 = """
    MERGE (u:User {email: $email, firstName: $firstName})
    MERGE (t:Tenant {name: $tenant})
    MERGE (u)-[:ATTENDED]->(t)
"""
    params_0 = {
        "email": form.email,
        "tenant": form.tenant,
        "firstName": form.firstName,
    }
    query_0_result = upload_to_neo4j(query_0, params_0)
    print(f"User and Tenant nodes creation result: {query_0_result}")

    # Remove prior relationships for User nodes already stored
    query_0a = """
    MATCH (u:User {email: $email})-[r]->(t:Tech)
    DELETE r
"""
    params_0a = {
        "email": form.email,
    }
    query_0a_result = upload_to_neo4j(query_0a, params_0a)
    print(f"Prior User-[r]->(t:Tech) relationships deleted result: {query_0a_result}")

    # Create new Tech nodes if needed
    all_tech.update(knows_tech)
    all_tech.update(interested_tech)
    all_tech.update(opensource)

    query_1a = """
    UNWIND $techStack AS tech
        MERGE (t:Tech {name: tech})
"""
    params_1a = {
        "techStack": list(all_tech),
    }
    query_1a_result = upload_to_neo4j(query_1a, params_1a)
    print(f"New tech creation result: {query_1a_result}")

    if len(knows_tech) > 0:
        query_1 = """
        MATCH (u:User {email: $email})
        WITH u
        UNWIND $techStack AS tech
            MATCH (t:Tech {name: tech})
            MERGE (u)-[:KNOWS]->(t)
    """
        params_1 = {
            "email": form.email,
            "techStack": knows_tech,
        }
        query_1_result = upload_to_neo4j(query_1, params_1)
        print(f"User-KNOWS-Tech relationship creation result: {query_1_result}")

    if len(interested_tech) > 0:
        query_2 = """
        MATCH (u:User {email: $email})
        WITH u
        UNWIND $learnTech AS tech
            MATCH (t:Tech {name: tech})
            MERGE (u)-[:INTERESTED_IN]->(t)
    """
        params_2 = {"email": form.email, "learnTech": interested_tech}
        query_2_result = upload_to_neo4j(query_2, params_2)
        print(f"User-INTERESTED_IN-Tech relationship creation result: {query_2_result}")

    if opensource is not None:
        # TODO: Create nodes if needed
        query_3 = """
        MATCH (u:User {email: $email})
        WITH u
        UNWIND $opensource AS tech
            MATCH (t:Tech {name: tech})
            MERGE (u)-[:LIKES]->(t)
    """
        params_3 = {
            "email": form.email,
            "opensource": opensource,
        }
        query_3_result = upload_to_neo4j(query_3, params_3)
        print(f"User-LIKES-Tech relationship creation result: {query_3_result}")

    return "OK", 200


@functions_framework.http
def import_form(request):

    # Optional Basic Auth
    basic_user = os.environ.get("BASIC_AUTH_USER", None)
    basic_password = os.environ.get("BASIC_AUTH_PASSWORD", None)
    if basic_user and basic_password:
        auth_header = request.headers.get("Authorization")
        if auth_header is None:
            return "Missing authorization credentials", 401
        request_username, request_password = decode(auth_header)
        if request_username != basic_user or request_password != basic_password:
            return "Unauthorized", 401

    payload = request.get_json(silent=True)

    if payload:
        try:
            form = FormData(**payload)
            return ingest_form(form)
        except Exception as e:
            return f"Invalid payload: {e}", 400
