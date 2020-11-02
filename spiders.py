"""
1.热搜入库
2.历史入库
3.详细入库
"""

from  selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
import time
import utils
import requests
import json



# 获取热搜数据
def get_hotdata():
    url = "https://voice.baidu.com/act/newpneumonia/newpneumonia/?from=osari_pc_1#tab2"
    # 浏览器
    chrome_options = Options()
    Options.headless = True
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    brower = Chrome(options=chrome_options)


    brower.get(url)
    html = brower.page_source
    # 放爬到的数据
    hotdata = []

    # 模拟浏览器点击
    btn = brower.find_element_by_xpath('//*[@id="ptab-2"]/div[1]/div/p/a')
    btn.click()
    time.sleep(1)

    btn = brower.find_element_by_xpath('//*[@id="ptab-0"]/div/div[2]/section/div')
    btn.click()
    time.sleep(1)

    # 获取数据
    content = brower.find_elements_by_xpath('//*[@id="ptab-0"]/div/div[2]/section/a/div/span[2]')
    for item in content:
        print(item.text)
        hotdata.append(item.text)
    return hotdata

# 热搜数据入库
def insert_hotdata():
    # 获取数据库连接
    conn,cursor = utils.get_conn()
    sql = 'insert into hotdata(dt,content) values(%s,%s)'
    sql_trunc = "truncate table hotdata"
    datas = get_hotdata()

    # dt当前时间
    dt = time.strftime("%Y-%m-%d %X")
    cursor.execute(sql_trunc)

    for item in datas:
        cursor.execute(sql, (dt,item))
        conn.commit()

    print("热搜数据插入成功！")
    utils.close(conn,cursor)



# 获取历史数据
def get_history():
    history = {}
    url = "https://view.inews.qq.com/g2/getOnsInfo?name=disease_other"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.100 Safari/537.36"
    }
    resp = requests.get(url,headers)
    jsondata = resp.text
    # 把json字符串转换为字典
    datas = json.loads(jsondata)

    data = json.loads(datas['data'])

    for day in data['chinaDayList']:
        # 时间
        dt = '2020.'+day['date']
        tup = time.strptime(dt, "%Y.%m.%d")
        dt = time.strftime("%Y-%m-%d", tup)
        # 确诊
        confirm = day['confirm']
        # 疑似
        suspect = day['suspect']
        # 出院
        heal = day['heal']
        # 死亡
        dead = day['dead']
        # f放入字典
        history[dt]={"confirm":confirm,"suspect":suspect,"heal":heal,"dead":dead}


    for dayadd in data["chinaDayAddList"]:
        dt = '2020.' + dayadd['date']
        tup = time.strptime(dt, "%Y.%m.%d")
        dt = time.strftime("%Y-%m-%d", tup)
        confirm_add = dayadd["confirm"]
        suspect_add = dayadd["suspect"]
        heal_add = dayadd["heal"]
        dead_add = dayadd["dead"]
        # 新加的字段更新到字典
        history[dt].update({"confirm_add":confirm_add,
                            "suspect_add":suspect_add,
                            "heal_add":heal_add,
                            "dead_add":dead_add})

    for item in history.keys():
        print(item, history[item])
    return history

#历史数据入库
def insert_history():
    # 获取数据库连接
    conn, cursor = utils.get_conn()
    history = get_history()
    # 查询数据库是否需要再次插入数据
    sql_query = 'select %s = (select ds from history order by ds desc limit 1)'
    sql_history = "truncate table history"

    history_dt = list(history)[-1]

    cursor.execute(sql_query, history_dt)
    res = cursor.fetchone()[0]

    sql = "insert into history values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    if not res:
        # 清空历史数据
        cursor.execute(sql_history)

        for k,v in history.items():
            cursor.execute(sql,[k,v.get("confirm"),v.get("confirm_add"),
                                v.get("suspect"),v.get("suspect_add"),
                                v.get("heal"),v.get("heal_add"),
                                v.get("dead"),v.get("dead_add")])
            conn.commit()
    else :
        print("history已是最新数据，无需更新！")

    utils.close(conn,cursor)


# 获取各省详细数据
def get_details():
    # 列表
    details = []
    url = "https://view.inews.qq.com/g2/getOnsInfo?name=disease_h5"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.100 Safari/537.36"
    }
    resp = requests.get(url,headers)
    jsondata = resp.text
    # json转换为字典
    datas = json.loads(jsondata)
    # 最终解析的数据
    data = json.loads(datas['data'])
    # 更新时间
    updatetime = data['lastUpdateTime']
    # 中国
    country = data['areaTree'][0]
    # 省份
    provinces = country['children']
    for province in provinces:
        # 省份名字
        pro_name = province['name']
        for city in province['children']:
            # 城市名字
            city_name = city['name']
            # 确诊
            confirm = city['total']['confirm']
            # 新增确诊
            confirm_add = city['today']['confirm']
            heal = city['total']['heal']
            dead = city['total']['dead']
            # print(city_name)
            details.append([updatetime,pro_name,city_name,confirm,confirm_add,heal,dead])
    print(details)
    return details

# 各省详细数据入库
def insert_details():
    conn,cursor = utils.get_conn()
    details = get_details()
    # 执行插入数据
    sql = 'insert into details(update_time,province,city,confirm,confirm_add,heal,dead) values(%s,%s,%s,%s,%s,%s,%s)'
    # 查询数据库中的数据是否需要更新，如果需要更新就更新，不需要就提示
    sql_query = 'select %s=(select update_time from details order by id desc limit 1)'
    cursor.execute(sql_query,details[0][0])
    if not cursor.fetchone()[0]:
        print("开始更新数据！")
        for item in details:
            cursor.execute(sql,item)
            conn.commit()

        print("数据更新成功！")
    else:
        print("已经是最新数据，不需要更新！")



if __name__ == "__main__":

    get_hotdata()
    insert_hotdata()

    get_history()
    insert_history()

    get_details()
    insert_details()