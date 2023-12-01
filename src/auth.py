import jwt
from .connectors.env import PUBLIC_KEY_AUTH

public_key = '-----BEGIN PUBLIC KEY-----\n' + PUBLIC_KEY_AUTH + '\n-----END PUBLIC KEY-----'

def do_auth(token: str):
    try:
        decoded = jwt.decode(token, public_key, algorithms=["RS256"])
        if decoded['role'] == "Dispatcher":
            return True
        return False
    except (jwt.exceptions.InvalidSignatureError, jwt.exceptions.DecodeError):
        return False