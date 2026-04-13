from dataclasses import dataclass


@dataclass(frozen=True)
class TuyaToken:
    access_token: str
    refresh_token: str
    expires_at_epoch: float
    uid: str
