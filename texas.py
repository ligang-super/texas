#coding:utf-8

import time
import card
from operator import itemgetter
from collections import defaultdict
from PlayCard import PlayerCardOption
from PlayCard import CARD_POINT_DICT
from PlayCard import DIAMONDS, HEARTS, SPADES, CLUBS, ALL_52_CARDS
from PlayCard import PRINT_LOG
#import MySQLdb
import pymysql
import lg_pymysql


def test_PlayerCardOption():
    po1 = PlayerCardOption(hand_cards=['heart-5', 'diamond-2'], hole_cards=['diamond-3', 'diamond-4', 'diamond-5'])
    po1.calc_cards()
    po1.print_res()

def test_cmp1():
    p1 = PlayerCardOption(hand_cards=['diamond-A', 'club-A'], hole_cards=['spade-Q', 'heart-9', 'diamond-7'])
    p2 = PlayerCardOption(hand_cards=['diamond-A', 'heart-10'], hole_cards=['spade-Q', 'heart-9', 'diamond-7'])
    p1.calc_cards()
    p2.calc_cards()
    p1.print_res()
    p2.print_res()
    print(card.cmp_card(p1, p2))

def test_cmp2():
    p1 = PlayerCardOption(hand_cards=['club-8', 'club-2'], hole_cards=['heart-Q', 'spade-Q', 'heart-8'])
    p2 = PlayerCardOption(hand_cards=['diamond-3', 'heart-3'], hole_cards=['heart-Q', 'spade-Q', 'heart-2'])
    p1.calc_cards()
    p2.calc_cards()
    p1.print_res()
    p2.print_res()
    print(card.cmp_card(p1, p2))

def _static_finnal_res(player_count, static_count):
    tbegin = time.time()
    ds_win = defaultdict(int)
    ds_cnt = defaultdict(int)
    for i in range(static_count):
        if i % 500 == 0:
            tcur = time.time()
            cost = round(tcur-tbegin, 2)
            print('%d: %s' % (i, cost))
        playing_cards = card.shuffle()
        r = card.deal(playing_cards, player_count)
        card.play(r)
        winner = r['winners'][0].hand_cards
        kstr = card.get_card_str(winner)

        #print(kstr)
        ds_win[kstr] += 1

        for _hand_card in r['hand_cards']:
            kstr = card.get_card_str(_hand_card)
            ds_cnt[kstr] += 1


    l = []
    for k, v in ds_cnt.items():
        win_count = ds_win.get(k, 0)
        if win_count:
            win_ratio = round(float(win_count)*100.0/float(v), 2)
        else:
            win_ratio = 0
        l.append({'card': k, 'win': win_count, 'all': v, 'ratio': win_ratio})

    l.sort(key=itemgetter('ratio'), reverse=True)

    print(l)


if __name__ == '__main__':
    print('Hello world!')

    dbw = lg_pymysql.MysqlClientPool(dbuser='root', dbpass='123456', pool_size=1, cls_name=lg_pymysql.MysqlConnection, host='localhost', port=3306, dbname='test_db', enc='utf8mb4')
    if not dbw:
        print("dbw is none")
        exit(-1)

    ret = dbw.insert_data(table='test_table', val_dict={'uid': 100}, has_crtime=True)
    print(ret)
    r1 = dbw.get_data_by_id(table='test_table', idx=1)
    print(r1)

    r2 = dbw.get_data(table='test_table', conditions=['uid > %d'], condvalues=[5])
    print(r2)



    #db = MySQLdb.connect("localhost", "root", "123456", "test_db", charset='utf8')
    #print(db)
    #cursor = db.cursor()
    #cursor.execute("SELECT * from test_db.test_table")
    #data = cursor.fetchone()
    #print(data)
    #db.close()
    #card.init_data()

    #_static_finnal_res(player_count=9, static_count=1000)

    #test_PlayerCardOption()
    #test_cmp()
    #test_cmp2()




