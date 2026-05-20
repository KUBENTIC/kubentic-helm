---
name: project-backend-spec
description: Backend dev spec for the validate-foresight endpoint that the Helm hook calls
metadata: 
  node_type: memory
  type: project
  originSessionId: e3488f8f-c7b9-4a44-af19-255ff3b1578d
---

## Backend Endpoint Spec: `/api/v1/auth/validate-foresight`

**Why:** This is the spec given to the backend developer to implement the subscription validation endpoint that the Helm pre-install hook calls.

```
Method:  GET
URL:     https://api.kubentic.ai/api/v1/auth/validate-foresight
Auth:    Authorization: Bearer <JWT>
Body:    none
```

### Response Contract

| Condition | HTTP | Body |
|---|---|---|
| Valid JWT + active subscription | 200 | `{"status": "active"}` |
| Missing / malformed token | 401 | `{"detail": "Invalid token"}` |
| Valid JWT but subscription cancelled | 403 | `{"detail": "Subscription inactive"}` |
| Valid JWT but user not in DB | 401 | `{"detail": "Token not recognized"}` |

### Logic (FastAPI sample)

```python
@router.get("/api/v1/auth/validate-foresight")
def validate_foresight(credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer())):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

    user = db.get_user_by_id(payload["sub"])
    if not user:
        raise HTTPException(status_code=401, detail="Token not recognized")
    if not user.subscription_active:
        raise HTTPException(status_code=403, detail="Subscription inactive")

    return {"status": "active"}
```

**How to apply:** When backend dev asks what to build, send them this spec exactly.
