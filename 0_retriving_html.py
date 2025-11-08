import requests
import time
from fake_useragent import UserAgent

url = "https://www.flipkart.com/triggr-trinity-2-dual-pairing-enc-fast-charge-50h-battery-rubber-finish-v5-3-bluetooth/p/itm03c651cfe2be4?pid=ACCH8Q2VGN7MHYHS&lid=LSTACCH8Q2VGN7MHYHSQSLI2G&marketplace=FLIPKART&store=0pm%2Ffcn%2Fgc3&srno=b_1_1&otracker=browse&fm=organic&iid=en_MYwYZQAvTrJ57LucwBti_yihZ-SGd6RV9w3EoZk5sdA7BdVINqLiWmBKR62_2uAPGmY7UH3-UJ0m5SfxA-TWWPUFjCTyOHoHZs-Z5_PS_w0%3D&ppt=browse&ppn=browse&ssid=07l2rjdu4g0000001762623596799"

session = requests.Session()
headers = {
    "User-Agent": UserAgent().random,
    "accept-language": "en-US,en;q=0.9",
    "Accept-encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/",
}

# r=requests.get(url, headers=session.headers)   //Abhi request se maari toh session use ni hua and loader hi aa gya baha.. Now we have to take request using session ....
time.sleep(2)
r=session.get(url, headers=headers)

with open("flipkart.html", "w") as f:
    f.write(r.text)