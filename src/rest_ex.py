import requests
import json

url = "https://jsonplaceholder.typicode.com/todos/99"
res = requests.get(url)
print(res.status_code)
print(res.json())

temp = {
    "userId": 1,
    "title": "vjy test",
    "completed": False
}

url = "https://jsonplaceholder.typicode.com/todos"
hdr = {"Content-Type":"application/json"}
res = requests.post(url, data=json.dumps(temp), headers=hdr)
print(res.status_code)
print(res.json())
