import json
payload= "{\"id\":1,\"name\":\"test\",\"description\":\"check\",\"weight\":100.2}"
data = json.loads(payload)
print(data)