from dataclasses import dataclass
from basicauth import decode
from neo4j import GraphDatabase, basic_auth
import functions_framework
import os
from openai import OpenAI
import json
import logging

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
        # model="gpt-3.5-turbo-1106",
        model="gpt-4o",
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

    prompt = """Return a JSON List of any application names, software technologies, or programming languages from a user statement of the following structure: 

    {
    "application": [...list of applications]
    }
    
    For example, return {"application": ["NextJS", "Django", "PostgresSQL"]} from the sentence "NextJS + Django + PostgreSQL" or the sentence "I am most comfortable with NextJS, Django, and PostgresSQL"."""

    response = CLIENT.chat.completions.create(
        model="gpt-4o",
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
            return driver.execute_query(query, params)
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
        additional_opensource = extract_name_description(form.openSource)
        additional_opensource_list = [
            d["name"]
            for d in additional_opensource
            if "name" in d and d["name"] not in additional_opensource
        ]
        opensource.extend(additional_opensource_list)

    except Exception as e:
        print(
            f"Error parsing openSource tech from form:{form}: {e}. Skipping openSource ingestion..."
        )

    all_tech.update(knows_tech)
    all_tech.update(interested_tech)
    all_tech.update(opensource)

    # Create User and Tenant records, and remove any prior tech relationships
    query_0 = """
    MERGE (u:User {email: $email})
    ON CREATE SET u.firstName = $firstName
    ON MATCH SET u.firstName = $firstName
    MERGE (te:Tenant {name: $tenant})
    MERGE (u)-[:ATTENDED]->(te)

    WITH u
    MATCH (u)-[r]->(t:Tech)
    DELETE r
"""
    params_0 = {
        "email": form.email,
        "tenant": form.tenant,
        "firstName": form.firstName,
    }
    _, query_0_result, _ = upload_to_neo4j(query_0, params_0)
    print(f"User and Tenant nodes creation result: {query_0_result.counters}")

    query_1a = """
    UNWIND $techStack AS tech
        MERGE (t:Tech {name: tech})
"""
    params_1a = {
        "techStack": list(all_tech),
    }
    _, query_1a_result, _ = upload_to_neo4j(query_1a, params_1a)
    print(f"New tech creation result: {query_1a_result.counters}")

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
        _, query_1_result, _ = upload_to_neo4j(query_1, params_1)
        print(
            f"User-KNOWS-Tech relationship creation result: {query_1_result.counters}"
        )

    if len(interested_tech) > 0:
        query_2 = """
        MATCH (u:User {email: $email})
        WITH u
        UNWIND $learnTech AS tech
            MATCH (t:Tech {name: tech})
            MERGE (u)-[:INTERESTED_IN]->(t)
    """
        params_2 = {"email": form.email, "learnTech": interested_tech}
        _, query_2_result, _ = upload_to_neo4j(query_2, params_2)
        print(
            f"User-INTERESTED_IN-Tech relationship creation result: {query_2_result.counters}"
        )

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
        _, query_3_result, _ = upload_to_neo4j(query_3, params_3)
        print(
            f"User-LIKES-Tech relationship creation result: {query_3_result.counters}"
        )

    return "OK", 200

    # print(f"aggregated tech labels: {all_tech}")

    # query = """
    # MERGE (u:User {email: $email})
    # ON CREATE SET u.firstName = $firstName
    # ON MATCH SET u.firstName = $firstName
    # MERGE (te:Tenant {name: $tenant})
    # MERGE (u)-[:ATTENDED]->(te)

    # WITH u
    # MATCH (u)-[r]->(t:Tech)
    # DELETE r

    # WITH u
    # UNWIND $allTech AS tech
    #     MERGE (t:Tech {name: tech})

    # WITH u
    # UNWIND $techStack AS tech
    #     MATCH (t:Tech {name: tech})
    #     MERGE (u)-[:KNOWS]->(t)

    # WITH u
    # UNWIND $learnTech AS tech
    #     MATCH (t:Tech {name: tech})
    #     MERGE (u)-[:INTERESTED_IN]->(t)

    # WITH u
    # UNWIND $opensource AS tech
    #     MATCH (t:Tech {name: tech})
    #     MERGE (u)-[:LIKES]->(t)
    # """

    # params = {
    #     "email": form.email,
    #     "tenant": form.tenant,
    #     "firstName": form.firstName,
    #     "techStack": list(knows_tech),
    #     "learnTech": list(interested_tech),
    #     "opensource": list(opensource),
    #     "allTech": list(all_tech),
    # }

    # records, summary, keys = upload_to_neo4j(query, params)
    # print(f"Query summary: {summary.counters}")

    # return "OK", 200


@functions_framework.http
def import_form(request):

    # Optional Basic Auth
    basic_user = os.environ.get("BASIC_AUTH_USER", None)
    basic_password = os.environ.get("BASIC_AUTH_PASSWORD", None)
    if basic_user and basic_password:
        auth_header = request.headers.get("Authorization")
        if auth_header is None:
            return "Missing authorization credentials", 401
        try:
            request_username, request_password = decode(auth_header)
            if request_username != basic_user or request_password != basic_password:
                return "Unauthorized", 401
        except Exception as e:
            logging.error(
                f"Problem parsing authorization header: {auth_header}: ERROR: {e}"
            )
            return f"Problem with Authorization credentials: {e}", 400

    payload = request.get_json(silent=True)

    if payload:
        try:
            form = FormData(**payload)
            return ingest_form(form)
        except Exception as e:
            return f"Invalid payload: {e}", 400
