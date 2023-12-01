import psycopg2
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, validator, Field
from typing import Union
from datetime import datetime, date

from . import database_request as db_req
from . import auth


def validate_date(value: str) -> date:
    if value is None:
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


class Deals(BaseModel):
    deal_id: str
    client_id: int
    leg_id: str


class CreateClient(BaseModel):
    client_id: int
    pass_id: int


class ErrorResponse(BaseModel):
    error: str
    error_description: str


class ClientData(BaseModel):
    # Client
    name: str = Field(..., max_length=255)
    middle_name: Union[str, None] = Field(None, max_length=255)
    surname: str = Field(..., max_length=255)
    known_as: Union[str, None] = Field(None, max_length=255)
    gender: Union[int, None] = None
    nationality: Union[str, None] = Field(None, max_length=255)
    place_of_birth: Union[str, None] = Field(None, max_length=255)
    date_of_birth: Union[date, None] = None
    # Pass
    pass_country: Union[str, None] = Field(None, max_length=255)
    pass_number: str = Field(..., max_length=255)
    pass_name: str = Field(..., max_length=255)
    pass_surname: str = Field(..., max_length=255)
    date_of_issue: Union[date, None] = None
    expiry_date: Union[date, None] = None
    is_default: Union[bool, None] = Field(True)
    pass_note: Union[str, None] = Field(None, max_length=255)

    _validate_dates = validator(
        'date_of_birth', 'date_of_issue', 'expiry_date', pre=True, allow_reuse=True,
    )(validate_date)


class ClientDataUpdate(BaseModel):
    # Client
    name: Union[str, None] = Field(None, max_length=255)
    middle_name: Union[str, None] = Field(None, max_length=255)
    surname: Union[str, None] = Field(None, max_length=255)
    known_as: Union[str, None] = Field(None, max_length=255)
    gender: Union[int, None] = None
    nationality: Union[str, None] = Field(None, max_length=255)
    place_of_birth: Union[str, None] = Field(None, max_length=255)
    date_of_birth: Union[date, None] = None
    # Pass
    pass_country: Union[str, None] = Field(None, max_length=255)
    pass_number: Union[str, None] = Field(None, max_length=255)
    pass_name: Union[str, None] = Field(None, max_length=255)
    pass_surname: Union[str, None] = Field(None, max_length=255)
    date_of_issue: Union[date, None] = None
    expiry_date: Union[date, None] = None
    is_default: Union[bool, None] = Field(True)
    pass_note: Union[str, None] = Field(None, max_length=255)

    _validate_dates = validator(
        'date_of_birth', 'date_of_issue', 'expiry_date', pre=True, allow_reuse=True,
    )(validate_date)


app = FastAPI()


@app.get("/clients", status_code=200)
async def get_client(response: Response, request: Request, surname: str = None, password: str = None, client_id: int = None) -> ClientData:
    auth_token = request.headers.get('Authorization')
    if auth.do_auth(auth_token) == False:
        response.status_code = 401
        return
    data = db_req.get_data(surname, password, client_id, "json")
    if len(data) == 2:
        response.status_code = 204
        return
    return Response(content=data, media_type="application/json")


@app.post("/deals", status_code=201)
async def create_deal(response: Response, request: Request, item: Deals):
    auth_token = request.headers.get('Authorization')
    if auth.do_auth(auth_token) == False:
        response.status_code = 401
        return
    try:
        db_req.insert_deals(item.deal_id, item.client_id, item.leg_id)
    except psycopg2.errors.UniqueViolation:
        response.status_code = 400
        return JSONResponse(content={"error": "Deal with this client and leg already exist!"})
    response.status_code = 201
    return


@app.post("/clients", status_code=201)
async def create_client(response: Response, request: Request, item: ClientData) -> CreateClient:
    auth_token = request.headers.get('Authorization')
    if not auth.do_auth(auth_token):
        response.status_code = 401
        return
    try:
        if (item.gender not in [1, 2, 3]) and item.gender is not None:
            response.status_code = 422
            return JSONResponse(content={"error": "Unknown gender! Check the DB",
                    "available gender": {"1": "male",
                                         "2": "female",
                                         "3": "unspecified"}})

        new_client_id = db_req.insert_client(item.name, item.middle_name, item.surname, item.known_as, item.gender,
                                             item.nationality, item.place_of_birth, item.date_of_birth)
        new_pass_id = db_req.insert_pass(new_client_id, item.pass_country, item.pass_number, item.pass_name,
                                         item.pass_surname, item.date_of_issue, item.expiry_date, item.is_default,
                                         item.pass_note)
    except psycopg2.errors.UniqueViolation:
        db_req.delete_client(new_client_id)
        response.status_code = 400
        return JSONResponse(content={"error": "Passport with this number already exist!"})
    response.status_code = 201
    return JSONResponse(content={"client_id": f"{new_client_id}",
                                 "pass_id": f"{new_pass_id}"})


@app.patch("/clients/{client_id}", status_code=201)
async def update_client(client_id: int, response: Response, request: Request, item: ClientDataUpdate) -> ClientData:
    auth_token = request.headers.get('Authorization')
    if not auth.do_auth(auth_token):
        response.status_code = 401
        return
    if (item.gender not in [1, 2, 3]) and item.gender is not None:
        response.status_code = 422
        return {"error": "Unknown gender! Check the DB",
                "available gender": {"1": "male",
                                     "2": "female",
                                     "3": "unspecified"}}

    df = db_req.get_data(None, None, client_id, "dict")[0]
    stored_item_model = ClientData(**df)
    update_data = item.dict(exclude_unset=True)
    updated_item = stored_item_model.copy(update=update_data)
    db_req.update_client(client_id, updated_item.name, updated_item.middle_name, updated_item.surname, updated_item.known_as, updated_item.gender,
                         updated_item.nationality, updated_item.place_of_birth, updated_item.date_of_birth)
    pass_update_result = db_req.update_pass(df['pass_id'], client_id, updated_item.pass_country, updated_item.pass_number, updated_item.pass_name, updated_item.pass_surname,
                                            updated_item.date_of_issue, updated_item.expiry_date, updated_item.is_default, updated_item.pass_note)
    if pass_update_result == 'UniqueViolationErr':
        response.status_code = 400
        return {"error": "Passport with this number already exist!"}
    response.status_code = 201
    return updated_item
