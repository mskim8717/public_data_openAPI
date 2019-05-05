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

# 버스노선정보조회서비스 - 노선번호목록조회
url_getRouteNoList = 'http://openapi.tago.go.kr/openapi/service/BusRouteInfoInqireService/getRouteNoList'

# 도착정보조회서비스
url_getSttnAcctoArvlPrearngeInfoList = 'http://openapi.tago.go.kr/openapi/service/ArvlInfoInqireService/getSttnAcctoArvlPrearngeInfoList'


def execute():
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
                    print('[검색시작] 정류소번호:',nodeno, '정류소ID:',nodeid)

                    queryParams = '?' + 'serviceKey=' + service_key + '&' + urlencode( {quote_plus( 'cityCode' ): '12',
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

                    find_routeno = "fail"
                    status = "fail"
                    existStatus = None
                    for row, data in enumerate( soup.find_all( 'item' ) ):
                        find_routeno = data.find( 'routeno' ).text
                        conn = sqlite3.connect( "node_update.db" )
                        with conn:
                            cur = conn.cursor()
                            cur.execute("select * from nodeInfo where routeid = ? AND nodeid = ? AND nodeno = ? AND routeno = ?", (routeid, nodeid, nodeno, find_routeno))
                            existStatus = cur.fetchone()
                            if existStatus is None:
                                cur.execute(
                                    "INSERT INTO nodeInfo(routeid, nodeid, nodeno, routeno) SELECT ?, ?, ?, ? WHERE NOT EXISTS(SELECT(routeid, nodeid, nodeno, routeno) FROM nodeInfo WHERE routeid = ? AND nodeid = ? AND nodeno = ? AND routeno = ?)",
                                    (routeid, nodeid, nodeno, find_routeno, routeid, nodeid, nodeno, find_routeno) )
                                conn.commit()
                                status = "new"
                            else:
                                status = 'already'
                            print( '[검색시작] 찾은노선번호:', find_routeno, data.find( 'arrtime' ).text, existStatus, status )
                        print( '[검색종료]' )
                        conn.close()
                    print( datetime.now(), routeno, routeid, nodeid, nodeno, 'find:',find_routeno, 'status:', status, existStatus, '\n' )
                    time.sleep(3)
    print( 'finish' )

while True:
    execute()
