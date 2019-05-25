# -*- coding: utf-8 -*-
import sys
import os
from urllib.request import urlopen, Request
from urllib.parse import urlencode, quote_plus, quote
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3
import time

busType = {'BRT': ['900', '990'], '시내': ['1004', '1005', '201', '203', '221', '11', '12', '13', '300', '910'],
           '마을': ['51', '52', '53', '61', '62', '63', '64', '65', '66', '661', '67', '69', '691', '16', '17', '31',
                  '32', '33', '34', '35', '36', '71', '72', '73', '74', '75', '76', '81', '82', '83', '84', '85', '86',
                  '91', '92', '93', '94', '95']}

routeTp = []
routeNo = []
for i, k in busType.items():
    routeTp.append( i )
    routeNo.append( k )

# print( routeTp )
# print( routeNo )

# data.go.kr 도시코드
cityCode = 12  # 세종시 도시코드 : 12

# data.go.kr 일반인증 키
service_key= '개인별 인증키 입력'

# 버스노선정보조회서비스 - 노선별경유정류소목록조회
url_getRouteAcctoThrghSttnList = 'http://openapi.tago.go.kr/openapi/service/BusRouteInfoInqireService/getRouteAcctoThrghSttnList'

# 버스정류소정보조회서비스 - 정류소번호목록조회
url_getSttnNoList = 'http://openapi.tago.go.kr/openapi/service/BusSttnInfoInqireService/getSttnNoList'

# 버스노선정보조회서비스 - 노선번호목록조회
url_getRouteNoList = 'http://openapi.tago.go.kr/openapi/service/BusRouteInfoInqireService/getRouteNoList'

# 도착정보조회서비스 - 정류소별도착예정정보목록조회
url_getSttnAcctoArvlPrearngeInfoList = 'http://openapi.tago.go.kr/openapi/service/ArvlInfoInqireService/getSttnAcctoArvlPrearngeInfoList'


def execute():
    try:
        for routeno_list in routeNo:
            print( routeno_list, '검색시작 ...' )
            for routeno in routeno_list: # 900, 990, 1004, 1005, ...
                print('[검색시작] 노선번호:', routeno)
                queryParams = '?' + 'serviceKey=' + service_key + '&' + urlencode( {quote_plus( 'cityCode' ): '12',
                                                                                    quote_plus( 'routeNo' ): routeno},
                                                                                   encoding='UTF-8' )
                request = Request( url_getRouteNoList + queryParams )
                response = urlopen( request )
                rescode = response.getcode()
                if (rescode == 200):
                    response_body = response.read()
                    soup = BeautifulSoup( response_body, "lxml" )
                else:
                    print( "Error Code:" + rescode )

                for row, data in enumerate( soup.find_all( 'item' ) ): # 900:SJB293000182,SJB293000183, 990:SJB...., ...
                    routeid = data.find( 'routeid' ).text
                    print('[검색시작] 노선ID:', routeid)
                    queryParams = '?' + 'serviceKey=' + service_key + '&' + urlencode( {quote_plus( 'cityCode' ): '12',
                                                                                        quote_plus( 'routeId' ): routeid,
                                                                                        quote_plus( 'numOfRows' ): 80},
                                                                                       encoding='UTF-8' )
                    request = Request( url_getRouteAcctoThrghSttnList + queryParams )
                    response = urlopen( request )
                    rescode = response.getcode()
                    if (rescode == 200):
                        response_body = response.read()
                        soup = BeautifulSoup( response_body, "lxml" )
                    else:
                        print( "Error Code:" + rescode )

                    for row, data in enumerate( soup.find_all( 'item' ) ):
                        nodeno = data.find( 'nodeno' ).text
                        nodeid = data.find( 'nodeid' ).text

                        queryParams = '?' + 'serviceKey=' + service_key + '&' + urlencode(
                            {quote_plus('cityCode'): '12',
                             quote_plus('nodeNo'): nodeno},
                            encoding='UTF-8')
                        request = Request(url_getSttnNoList + queryParams)
                        response = urlopen(request)
                        rescode = response.getcode()
                        if (rescode == 200):
                            response_body = response.read()
                            soup = BeautifulSoup(response_body, "lxml")
                        else:
                            print("Error Code:" + rescode)

                        nodenm = soup.find('nodenm').text
                        gpslati = soup.find('gpslati').text
                        gpslong = soup.find('gpslong').text

                        conn = sqlite3.connect( server_path + "node_update.db" )
                        with conn:
                            cur = conn.cursor()
                            cur.execute("select * from nodeInfo where routeno = ? AND routeid = ? AND nodeno = ? AND nodeid = ? AND nodenm = ? AND gpslati = ? AND gpslong = ?",(routeno, routeid, nodeno, nodeid, nodenm, gpslati, gpslong) )
                            dbExistStatus = cur.fetchone() # 데이터베이스에 이미 업데이트 된 상태인지 확인
                        conn.close()

                        print('[검색시작] 노선번호:', routeno, '정류소번호:',nodeno, '정류소ID:',nodeid, 'DB검색결과:', dbExistStatus)

                        if dbExistStatus is None:
                            print('[DB응답] 신규 데이터베이스 업데이트.')
                            queryParams = '?' + 'serviceKey=' + service_key + '&' + urlencode(
                                {quote_plus( 'cityCode' ): '12',
                                 quote_plus( 'nodeId' ): nodeid},
                                encoding='UTF-8' )
                            request = Request( url_getSttnAcctoArvlPrearngeInfoList + queryParams )
                            response = urlopen( request )
                            rescode = response.getcode()
                            if (rescode == 200):
                                response_body = response.read()
                                soup = BeautifulSoup( response_body, "lxml" )
                            else:
                                print( "Error Code:" + rescode )

                            find_routeno = "검색결과 없음"
                            print('resultmsg:', soup.find( 'resultmsg' ).text)
                            if soup.find( 'resultmsg' ).text == 'NORMAL SERVICE.':
                                for row, data in enumerate( soup.find_all( 'item' ) ):
                                    find_routeno = data.find( 'routeno' ).text # '도착정보조회서비스'를 통해 검색 된 해당 정류장을 지나가는 노선번호
                                    conn = sqlite3.connect( "node_update.db" )
                                    with conn:
                                        cur = conn.cursor()
                                        cur.execute(
                                                "INSERT INTO nodeInfo(updateTime, routeno, routeid, nodeno, nodeid, nodenm, gpslati, gpslong, find_routeno) SELECT ?, ?, ?, ?, ?, ?, ?, ?, ? WHERE NOT EXISTS(SELECT(updateTime, routeno, routeid, nodeno, nodeid, nodenm, gpslati, gpslong, find_routeno) FROM nodeInfo WHERE routeno = ? AND routeid = ? AND nodeno = ? AND nodeid = ? AND nodenm = ? AND gpslati = ? AND gpslong = ? AND find_routeno = ?)",
                                                (str(datetime.now()), routeno, routeid, nodeno, nodeid, nodenm, gpslati, gpslong, find_routeno, routeno, routeid, nodeno, nodeid, nodenm, gpslati, gpslong, find_routeno) )
                                        conn.commit()
                                        print( '[검색시작] 찾은노선번호:', find_routeno, data.find( 'arrtime' ).text )
                                    print( '[검색종료]' )
                                    conn.close()
                                print( '업데이트 시간:', datetime.now(), '노선번호:', routeno, '노선ID:', routeid, '정류소번호:', nodeno,
                                       '정류소ID:', nodeid, '정류소명:', nodenm, '위도:',gpslati, '경도:',gpslong, '검색버스정보:', find_routeno, '\n' )
                                if find_routeno != 'fail':
                                    time.sleep( 10 )
                                else:
                                    time.sleep(3)
                            else:
                                print( '[검색 제한으로 인한 프로그램 종료]' )
                                sys.exit()
                        else:
                            print('[DB응답] 이미 데이터베이스에 등록이 되어 있습니다.')
        print( '[프로그램 종료]' )
        sys.exit()
    except:
        print('exception')

while True:
    execute()
