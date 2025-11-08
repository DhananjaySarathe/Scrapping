import requests
import time
from fake_useragents import UserAgent

url = "https://www.flipkart.com/audio-video/headset/pr?sid=0pm%2Cfcn&p%5B%5D=facets.connectivity%255B%255D%3DBluetooth&sort=popularity&p%5B%5D=facets.rating%255B%255D%3D3%25E2%2598%2585%2B%2526%2Babove&p%5B%5D=facets.rating%255B%255D%3D4%25E2%2598%2585%2B%2526%2Babove&p%5B%5D=facets.price_range.from%3D599&p%5B%5D=facets.price_range.to%3DMax&p%5B%5D=facets.headphone_type%255B%255D%3DTrue%2BWireless&param=86&hpid=WqCPtE2MbDEYEbYbttxC1qp7_Hsxr7nj65vMAAFK1c%3D&ctx=eyJjYXQiOiJ1ZGV4dCI6eyJhdHRyaWJ1dGVzIjp7InZhbHVlIjoiVkFMVUVFQ0FMTElOVCJ9fX0%3D"

session = requests.Session()
session.headers.update({
    "User-Agent": UserAgent().random,
    "accept-language": "en-US,en;q=0.9",
    "Accept-encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/",
})

# r=requests.get(url, headers=session.headers)   //Abhi request se maari toh session use ni hua and loader hi aa gya baha.. Now we have to take request using session ....
r=session.get(url, headers=session.headers)

with open("flipkart.html", "w") as f:
    f.write(r.text)