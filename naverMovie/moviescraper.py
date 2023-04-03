from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import pymysql
from datetime import date, timedelta

#passwd에 본인 mysql 비밀번호 넣기
conn = pymysql.connect(host='127.0.0.1', user='root', passwd='', db='mysql', charset='utf8')

cur = conn.cursor()
cur.execute('USE scraping')

#페이지의 숫자를 입력하면 그 페이지에 해당하는 목록 링크들을 반환
def getCodesFromPage(pagenum, standardDay):
    html = urlopen('https://movie.naver.com/movie/sdb/rank/rmovie.naver?sel=pnt&date='+standardDay+'&page='+str(pagenum))
    bs = BeautifulSoup(html, 'html.parser')
    codes = []
    list = bs.find('tbody').findAll('tr')

    for tr in list:
        movie=tr.find('a')
        if(movie == None):
        #목차 앞뒤로 목록 indicator들이 있는데 이들은 tr안에 들어가지만 영화리스트는 아니다. 따라서 넘김
            continue
        elif(float(tr.find('td',{'class':'point'}).get_text())<9.0):
            break
        else:
            link = movie.attrs['href']
            codes.append(link[30:])

    return codes

def insertMovieFromCodes(codes):
    for code in codes:
        html = urlopen('https://movie.naver.com/movie/bi/mi/basic.naver?code='+code)
        bs = BeautifulSoup(html, 'html.parser')


        validMovie = True
        #조건1. 공연실황 영화 제거(ex. 아임 히어로 더 파이널, 장민호 드라마 최종회)
        try:
            types = bs.find('dt',{'class':'step1'}).next_sibling.next_sibling.findAll('a')
        except:
            print("error in condition 1, "+code)
            continue
        for type in types:
            if(type.get_text()=='공연실황'):
                validMovie = False
                break
        if(validMovie==False):
            continue
        #조건2. 네티즌 리뷰 수가 3천 미만이면 제거
        try:
            numReview = int(bs.find('span', {'class':'user_count'}).get_text()[3:-1].replace(',',''))
        except:
            print("error in condition 2, "+code)
            continue

        if(numReview<3000):
            validMovie = False
        if(validMovie==False):
            continue

        #rating을 먼저 스크랩 해보고 특정값(9.0) 아래면 포함시키지 않는다
        ratingTag = bs.find('div', {'class':'score score_left'}).find('a', {'id':"pointNetizenPersentBasic"}).findAll('em')
        rating=''
        for r in ratingTag:
            rating += r.get_text()
        
        if(float(rating)<9.0):
            print("In ranking list. but under 9.0, "+code)
            continue

        title = bs.find('div',{'id':'content'}).find('div', {'class':'mv_info'}).find('a',{'href':'./basic.naver?code='+code}).get_text()

        director = (bs.find('div',{'id':'content'}).find('div', {'class':'mv_info'}).find('dt').next_sibling.next_sibling.get_text()).strip()

        
        cur.execute('INSERT INTO movielist (title, director, rating) VALUES (%s, %s, %s)', (title, director, rating))
        conn.commit()
    return True
        


def getStandardDay():
    #YYYYMMDD형식의 기준일을 문자열로출력한다.
    #기준일은 어제이다. ex) 오늘이 3월 13일이면 기준일은 3월 12일
    today = date.today()
    yesterday = date.today() - timedelta(1)
    return(str(yesterday.strftime("%Y%m%d")))


numPage=1 
codes = getCodesFromPage(numPage, getStandardDay())
while(codes):
    insertMovieFromCodes(codes)
    numPage +=1
    codes = getCodesFromPage(numPage, getStandardDay())


cur.close()
conn.close()