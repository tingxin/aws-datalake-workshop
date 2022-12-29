import csv
from random import randint
from datetime import datetime, timedelta

header = ['user_id', 'sex', 'age', 'view_duration', 'view_times', 'event_time',
          'resource_content', 'resource_image_url']

resource_titles = [
    "这只狗狗真可爱啊",
    "今天天气真不错",
    "这家餐厅超赞，尤其是牛排",
    "唯有美食能让我身心愉悦",
    "哎呀，开车太难了，我还是选择公共交通吧",
    "电脑，平板，手机，都是我的最爱，这款手机我可是等了好久",
    "这滑板车不错，谁知道这是什么牌子的吗"]


resource_titles_url = [
    "https://ts1.cn.mm.bing.net/th/id/R-C.d4a2d4dd30fcbfc350c0f43fd7e1117e?rik=%2frh3RTmCEVkCMg&riu=http%3a%2f%2fimg.ewebweb.com%2fuploads%2f20190821%2f12%2f1566361031-zDIyQuLXOj.jpeg&ehk=4sKFgIIKa6LjXAMT0BxwLbUZi%2fGXA%2f%2bgiPTjtftVEz8%3d&risl=&pid=ImgRaw&r=0",
    "https://img.zcool.cn/community/0193e456dfbe1d32f875520f1e697f.JPG@1280w_1l_2o_100sh.jpg",
    "https://tse1-mm.cn.bing.net/th/id/OIP-C.Mrn1lHjLlFHFv6ABgfFZgAHaE8?pid=ImgDet&rs=1",
    "https://tse1-mm.cn.bing.net/th/id/OIP-C.IDoyUmvAnzFYUoo-HCrLSwHaHa?pid=ImgDet&rs=1",
    "https://ts1.cn.mm.bing.net/th/id/R-C.9736acac1e94833d9fff58d648d3e2d9?rik=is22szBwCXHuNQ&riu=http%3a%2f%2fupload.site.cnhubei.com%2f2013%2f0204%2fthumb_940__1359966917548.jpg&ehk=BHVmgcmdAsGeDoLA7xOeVFy60Q3UDiqy8LfkESpkxV4%3d&risl=&pid=ImgRaw&r=0",
    "https://l.b2b168.com/2020/06/20/12/202006201255016329204.jpg",
    "https://ts1.cn.mm.bing.net/th/id/R-C.c72deaecbe4e5700da0ae0f13af2fa9a?rik=%2fr9ctzfLWbh3zA&riu=http%3a%2f%2fwww.whatsupmag.cn%2fuploadfile%2f2020%2f0818%2f20200818075130491.jpg&ehk=F%2bvG4YXoG04Wxns1kIoTjbSWxeUbepS5PfCTlrAFwvs%3d&risl=&pid=ImgRaw&r=0"]

sexs = ["man", "female"]
actions = ["click", "post"]
now = datetime.now()
with open('result.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    for i in range(0, 1000):
        user_id = randint(0, 100)
        sex = sexs[randint(0, 1)]
        age = randint(15, 50)
        view_duration = randint(1000, 600000)
        view_times = randint(1, 5)
        event_time = (now + timedelta(randint(-5, 5))).strftime("%Y-%m-%d")
        resource_content = resource_titles[randint(0, 6)]
        resource_url = resource_titles_url[randint(0, 6)]
        writer.writerow([user_id, sex, age, view_duration, view_times, event_time,
                         resource_content, resource_url])
