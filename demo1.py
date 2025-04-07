# coding=utf-8
import time
import traceback
from DrissionPage import Chromium, ChromiumOptions
import re
from urllib.parse import urlencode
from lxml import etree
import tls_client
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys


class PaiMai:
    def __init__(self, max_page, name_dict, max_Thread=5):
        self.cookies = {}
        self.res = {name: {} for name in name_dict.keys()}
        self.name_dict = name_dict
        self.max_page = max_page
        self.max_Thread = max_Thread

    @staticmethod
    def wait_click(page, xp, timeout):
        try:
            page.wait.ele_displayed(xp, timeout)
            page.ele(xp).click()
            return True
        except Exception:
            print(f'wait False {xp}')
            raise Exception('程序异常')

    def get_project_id(self, itemUrl):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "cache-control": "max-age=0",
                "sec-ch-ua": "\"Chromium\";v=\"134\", \"Not:A-Brand\";v=\"24\", \"Google Chrome\";v=\"134\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "upgrade-insecure-requests": "1",
                "sec-fetch-site": "none",
                "sec-fetch-mode": "navigate",
                "sec-fetch-user": "?1",
                "sec-fetch-dest": "document",
                "accept-language": "zh-CN,zh;q=0.9",
                "priority": "u=0, i"
            }

            session = tls_client.Session(
                random_tls_extension_order=True
            )
            itemUrl = itemUrl.split('?')[0]
            response = session.get(itemUrl, headers=headers, cookies=self.cookies, timeout_seconds=15, allow_redirects=True)
            if response.status_code == 200:
                project_id = re.search('project_id=(.*?)&', response.text)
                if project_id:
                    return project_id.group(1)

        except Exception as e:
            pass

    def get_content(self, project_id, _id):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-ch-ua": "\"Chromium\";v=\"134\", \"Not:A-Brand\";v=\"24\", \"Google Chrome\";v=\"134\"",
                "sec-ch-ua-mobile": "?0",
                "sec-fetch-site": "same-site",
                "sec-fetch-mode": "no-cors",
                "sec-fetch-dest": "script",
                "referer": "https://sf-item.taobao.com/",
                "accept-language": "zh-CN,zh;q=0.9"
            }
            url = "https://detail-ext.taobao.com/json/get_project_desc_content.do"
            params = {
                "project_id": project_id,
                "id": _id
            }
            session = tls_client.Session(
                random_tls_extension_order=True
            )

            response = session.get(url, headers=headers, cookies=self.cookies, params=params, timeout_seconds=15)
            if response.status_code == 200:
                content = ''.join(etree.HTML(response.json()['content']).xpath('.//text()')).replace(' ', '')
                return content
        except Exception as e:
            pass

    def get_res(self, name, item):
        d = {}
        _id = str(item["id"])
        d['title'] = item["title"]
        d['initialPrice'] = item["initialPrice"]
        d['consultPrice'] = item["consultPrice"]
        d['start'] = datetime.fromtimestamp(int(item["start"] / 10e2)).strftime('%Y-%m-%d %H:%M:%S')
        d['end'] = datetime.fromtimestamp(item["end"] / 10e2).strftime('%Y-%m-%d %H:%M:%S')

        itemUrl = 'https:' + item["itemUrl"]
        print(f'正在获取{_id}详情数据 {itemUrl}')
        d['itemUrl'] = itemUrl
        project_id = self.get_project_id(itemUrl)
        d['project_id'] = project_id
        d['content'] = self.get_content(project_id, _id)
        self.res[name][_id] = d

    def parse_html(self, name, html_list):
        for html in html_list:
            html = etree.HTML(html)
            text = html.xpath('//*[@id="sf-item-list-data"]/text()')

            if text:
                data = json.loads(text[0])
                data = data["data"]
                with ThreadPoolExecutor(max_workers=self.max_Thread) as executor:
                    for item in data:
                        executor.submit(self.get_res, name, item)

    def start(self, name, local_port=8011):
        html_list = []
        max_page = 1
        user_data_path = f'C:/Users{local_port}'
        browser = None
        cookies = None
        params = {
            'q': name
        }
        q_name = urlencode(params)
        try:
            url = f"https://sf.taobao.com/item_list.htm?_input_charset=utf-8&{q_name}&spm=a213w.3064813.search_index_input.1&keywordSource=5"

            co = ChromiumOptions().set_paths(local_port=local_port, user_data_path=user_data_path)
            co.incognito()  # 匿名模式
            # co.headless()  # 无头模式
            co.no_imgs(True).mute(True)
            co.set_argument('--no-sandbox')  # 无沙盒模式
            browser = Chromium(co)
            browser.set.load_mode.none()
            page = browser.latest_tab
            page.get(url, timeout=15)

            if page.wait.eles_loaded('x://button[@class="J_PageSkipSubmit" and text()="确定"]', timeout=30):
                time.sleep(5)
                if page.ele('x://a[@class="link-wrap"]', timeout=10):
                    h = etree.HTML(page.html)
                    page_skip = h.xpath('//span[@class="page-skip"]/em/text()')
                    if page_skip:
                        max_page = int(page_skip[0])
                        if max_page > self.max_page:
                            max_page = self.max_page

                    html_list.append(page.html)
                    cu_page = 1
                    if self.name_dict[name]:
                        while cu_page < max_page:
                            xp = 'x://a[@class="next"]'
                            if self.wait_click(page, xp, 5):
                                h = etree.HTML(page.html)
                                c_page = h.xpath('//span[@class="current"]/text()')
                                if c_page:
                                    cu_page = int(c_page[0])
                                else:
                                    print('获取当前页面失败')
                                    break

                            else:
                                print('初始页面加载异常')
                                break
                    cookies = {i['name']: i['value'] for i in page.cookies()}

            browser.quit()
        except Exception:
            if browser:
                browser.quit()
            print(traceback.format_exc())
        return html_list, cookies

    def run(self):
        for name in self.name_dict.keys():
            html_list, self.cookies = self.start(name)
            if self.cookies:
                self.parse_html(name, html_list)
            else:
                print(name, '初始页面请求失败')


if __name__ == '__main__':
    # 初次采集  1
    # 更新采集  2
    # 补录公告数据 3
    # task_type = int(sys.argv[1])
    task_type = 1
    with open('task_list.json', 'r', encoding='utf-8') as f:
        name_list = json.loads(f.read())

    task_map = {
        1: '初次采集',
        2: '增量采集',
        3: '补录公告数据'
    }
    print(f'当前任务列表 {name_list}  任务类型 {task_map[task_type]}')
    if task_type == 1:
        name_dict = {name: True for name in name_list}
        p = PaiMai(10, name_dict)
        p.run()
        print(p.res)
        for k, v in p.res.items():
            with open(k + '.json', 'w', encoding='utf-8') as f:
                json.dump(v, f, indent=4, ensure_ascii=False)

    elif task_type == 2:
        name_dict = {name: False for name in name_list}
        p = PaiMai(10, name_dict)
        p.run()
        print(p.res)
        for k, v in p.res.items():
            if Path(k + '.json').exists():
                with open(k + '.json', 'r', encoding='utf-8') as f:
                    res = f.read()
                    if res:
                        data = json.loads(res)
                    else:
                        data = {}
                    for k1, v1 in v.items():
                        if k1 not in data:
                            data[k1] = v1

                with open(k + '.json', 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4, ensure_ascii=False)

            else:
                with open(k + '.json', 'w', encoding='utf-8') as f:
                    json.dump(v, f, indent=4, ensure_ascii=False)

    elif task_type == 3:
        name_dict = {name: False for name in name_list}
        p = PaiMai(10, name_dict)
        c = 0
        with open('cookies.txt', 'r', encoding='utf-8') as f:
            text = f.read()
            if text:
                cookies = json.loads(text)
                p.cookies = cookies
            else:
                pass
        max_count = 1  # 最大重试次数 超过次数可能被封Ip, 需要稍后重试
        for name in name_list:
            if Path(name + '.json').exists():
                with open(name + '.json', 'r', encoding='utf-8') as f:
                    res = f.read()
                    if res:
                        data = json.loads(res)
                    else:
                        data = {}
                    try:
                        for k, v in data.items():
                            project_id = v['project_id']
                            content = v['content']
                            itemUrl = v['itemUrl']
                            if not project_id:
                                print(f'正在获取{k}详情数据 {itemUrl}')
                                project_id = p.get_project_id(itemUrl)
                                if not project_id:
                                    print('未获取到标的公告ID数据， 检查是否被风控，需要访问详情数据页面手动登录')
                                    c += 1
                                    if c > max_count:
                                        print('超过最大重试次数，退出程序。 请稍后重试')
                                        exit()
                                else:
                                    v['project_id'] = project_id
                                    print(f'正在获取{k}标的公告原文数据 {itemUrl}')
                                    content = p.get_content(project_id, k)
                                    if not content:
                                        print('未获取到标的公告原文数据， 需要访问详情数据页面手动登录')
                                        c += 1
                                        if c > max_count:
                                            print('超过最大重试次数，退出程序。 请稍后重试')
                                            exit()
                                    else:
                                        v['content'] = content
                            else:
                                if not content:
                                    print(f'正在获取{k}标的公告原文数据 {itemUrl}')
                                    content = p.get_content(project_id, k)
                                    if not content:
                                        print('未获取到标的公告原文数据， 需要访问详情数据页面手动登录')
                                        c += 1
                                        if c > max_count:
                                            print('超过最大重试次数，退出程序。 请稍后重试')
                                            exit()
                                    else:
                                        v['content'] = content

                    except:
                        pass
                    finally:
                        with open(name + '.json', 'w', encoding='utf-8') as f:
                            json.dump(data, f, indent=4, ensure_ascii=False)
            else:
                print(f'任务异常， {name}.json文件不存在，无法进行补录任务')