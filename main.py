from dataclasses import dataclass
from basicauth import decode
from neo4j import GraphDatabase, basic_auth
import functions_framework
import os
import ollama
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


# CLIENT = OpenAI(
#     api_key=OPENAI_KEY,
# )


def extract_name_description(sentence: str) -> str:
    # prompt = f'Return a JSON dictionary with the keys "name" and "description" from a user statement about their favorite Open Source project. For example, return {{"name":"TensorFlow", "description":"An end-to-end open source platform for machine learning"}} from the sentence: "My favorite open source project is TensorFlow. It is an end-to-end open source platform for machine learning."'

    # response = ollama.chat(
    #     model="llama3:70b",
    #     options={"temperature": 0.0},
    #     format="json",
    #     messages=[
    #         {
    #             "role": "system",
    #             "content": prompt,
    #         },
    #         {
    #             "role": "user",
    #             "content": sentence,
    #         },
    #     ],
    # )

    prompt = f'Return a JSON dictionary with the keys "name" and "description" from a user statement about their favorite Open Source project. For example, return {{"name":"TensorFlow", "description":"An end-to-end open source platform for machine learning"}} from the sentence: "My favorite open source project is TensorFlow. It is an end-to-end open source platform for machine learning".\n\n user statement: {sentence}'

    response = ollama.generate(
        model="llama3", options={"temperature": 0.0}, format="json", prompt=prompt
    )

    # print(f"response: {response['message']['content']}")

    # response = CLIENT.chat.completions.create(
    # model="gpt-3.5-turbo-1106",
    # response_format = {"type":"json_object"},
    # messages=[
    #     {
    #         "role":"system",
    #         "content": prompt
    #     },
    #     {
    #         "role":"user",
    #         "content": sentence
    #     }
    # ],
    # max_tokens=200,
    # n=1,
    # stop=None,
    # temperature=0.0,
    # )

    print(f"response: {response}.")
    response_string = response["response"]
    extracted = json.loads(response_string)

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

    prompt = f'Return a JSON List of any software technologies or programming languages from a user statement. For example, return ["NextJS", "Django", "PostgresSQL"] from the sentence "NextJS + Django + PostgreSQL" or the sentence "I am most comfortable with NextJS, Django, and PostgresSQL".\n\n user statement: {sentence}'

    response = ollama.generate(
        model="llama3", options={"temperature": 0.0}, format="json", prompt=prompt
    )

    print(f"Topics Response from LLM: {response}. Type: {type(response)}")
    try:
        response_string = response["response"]
        response_dict = json.loads(response_string)
        keys = response_dict.keys()
        a_key = next(iter(keys))
    except Exception as e:
        print()

    # a_key = keys.first()
    extracted = response_dict[a_key]

    # LLM may return a dictionary instead of the requested list
    if isinstance(extracted, dict):
        primary_key = list(extracted.keys())[0]
        extracted = extracted[primary_key]

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

    print(f"Extracting topics from form techstack: {form.techStack} ...")
    try:
        knows_tech = extract_topics(form.techStack)
    except Exception as e:
        print(
            f"Error parsing techStack from form:{form}: {e}.\n\n Skipping techStack ingestion..."
        )
        knows_tech = []

    print(f"Extracting topics from form learnTech: {form.learnTech} ...")
    try:
        interested_tech = extract_topics(form.learnTech)
    except Exception as e:
        print(
            f"Error parsing learnTech from form:{form}: {e}.\n\n Skipping learnTech ingestion..."
        )
        interested_tech = []

    print(f"Extracting openSource from form openSource: {form.openSource} ...")
    try:
        opensource = extract_name_description(form.openSource)
    except Exception as e:
        print(
            f"Error parsing openSource from form:{form}: {e}.\n\n Skipping openSource ingestion..."
        )
        opensource = None

    print(f"Inserting/updating user Node... ")
    query_0 = """
    MERGE (u:User {email: $email}) 
    SET u.firstName = $firstName
    MERGE (t:Tenant {name: $tenant})
    MERGE (u)-[r:ATTENDED]->(t)
    RETURN count(r) as relationships_created
"""
    params_0 = {
        "email": form.email,
        "tenant": form.tenant,
        "firstName": form.firstName,
    }
    query_0_result = upload_to_neo4j(query_0, params_0)
    print(f"User and Tenant nodes creation result: {query_0_result}")

    if len(knows_tech) > 0:
        print(f"Inserting KNOWS relationships between user and tech nodes...")
        query_1 = """
        MATCH (u:User {email: $email})
        WITH u
        UNWIND $techStack AS tech
            MERGE (t:Tech {name: tech})
            MERGE (u)-[r:KNOWS]->(t)
        RETURN count(r) as relationships_created
    """
        params_1 = {
            "email": form.email,
            "techStack": knows_tech,
        }
        query_1_result = upload_to_neo4j(query_1, params_1)
        print(f"User-KNOWS-Tech relationship creation result: {query_1_result}")

    if len(interested_tech) > 0:
        print(f"Inserting INTERESTED_IN relationships between user and tech nodes...")
        query_2 = """
        MATCH (u:User {email: $email})
        WITH u
        UNWIND $learnTech AS tech
            MERGE (t:Tech {name: tech})
            MERGE (u)-[r:INTERESTED_IN]->(t)
        RETURN count(r) as relationships_created
    """
        params_2 = {"email": form.email, "learnTech": interested_tech}
        query_2_result = upload_to_neo4j(query_2, params_2)
        print(f"User-INTERESTED_IN-Tech relationship creation result: {query_2_result}")

    if opensource is not None:
        print(f"Inserting LIKES relationship between user and openSource node...")
        query_3 = """
        MATCH (u:User {email: $email})
        MERGE (t:Tech {name: $name})
        MERGE (u)-[r:LIKES]->(t)
        RETURN count(r) as relationships_created
    """
        params_3 = {
            "email": form.email,
            "name": opensource.name,
        }
        query_3_result = upload_to_neo4j(query_3, params_3)
        print(f"User-LIKES-Tech relationship creation result: {query_3_result}")

    return "Success", 200


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
