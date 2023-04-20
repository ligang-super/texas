#coding:utf-8

import random
from operator import itemgetter
from collections import defaultdict
from PlayCard import PlayerCardOption
from PlayCard import CARD_POINT_DICT
from PlayCard import DIAMONDS, HEARTS, SPADES, CLUBS, ALL_52_CARDS
from PlayCard import PRINT_LOG

def shuffle():
    new_cards = ALL_52_CARDS
    shuffled_cards = []
    for i in range(52):
        idx = random.randint(0, 52-i-1)
        shuffled_cards.append(new_cards[idx])
        new_cards = new_cards[0:idx] + new_cards[idx+1:]

    if PRINT_LOG:
        print(len(shuffled_cards))
    return shuffled_cards

def init_data():
    idx = 1
    for i in DIAMONDS:
        CARD_POINT_DICT[i] = idx
        idx+=1
    idx = 1
    for i in HEARTS:
        CARD_POINT_DICT[i] = idx
        idx += 1
    idx = 1
    for i in SPADES:
        CARD_POINT_DICT[i] = idx
        idx += 1
    idx = 1
    for i in CLUBS:
        CARD_POINT_DICT[i] = idx
        idx += 1
    CARD_POINT_DICT['diamond-A'] = 14
    CARD_POINT_DICT['heart-A'] = 14
    CARD_POINT_DICT['spade-A'] = 14
    CARD_POINT_DICT['club-A'] = 14

def deal(shuffled_cards, player_count):
    r = {
        'hand_cards': [],
        'flops': [],
        'turn': '',
        'river': '',
        'players': [],
        'winners': []
         }

    idx = 0
    for i in range(player_count):
        r['hand_cards'].append([shuffled_cards[idx]])
        idx += 1
    for i in range(player_count):
        r['hand_cards'][i].append(shuffled_cards[idx])
        idx += 1
    # cut1
    idx += 1
    for i in range(3):
        r['flops'].append(shuffled_cards[idx])
        idx+=1
    # cut2
    idx += 1
    r['turn'] = shuffled_cards[idx]
    idx += 1

    # cut3
    idx += 1
    r['river'] = shuffled_cards[idx]
    idx += 1

    r['hole_cards'] = r['flops'] + [r['turn'], r['river']]

    return r

def print_all(d):
    pidx = 1
    for i in d['hand_cards']:
        print('player%d: %s' % (pidx, i))
        pidx += 1
    print('flops: %s' % d['flops'])
    print('turn: %s' % d['turn'])
    print('river: %s' % d['river'])

def play(d):
    d['res'] = []
    for hand_card in d['hand_cards']:
        _calc_cards = [hand_card[0], hand_card[1]] + d['hole_cards']
        p0_list = []
        p1_list = []
        p2_list = []
        for c1 in range(3):
            for c2 in range(c1+1, 4):
                for c3 in range(c2+1, 5):
                    for c4 in range(c3+1, 6):
                        for c5 in range(c4+1, 7):
                            all_cards = [_calc_cards[c1], _calc_cards[c2], _calc_cards[c3], _calc_cards[c4], _calc_cards[c5]]
                            p = PlayerCardOption(hand_cards=hand_card, hole_cards=d['hole_cards'], all_cards=all_cards)
                            p.calc_cards()
                            if c5 == 4:
                                p0_list.append(p)
                            elif c5 != 6:
                                p1_list.append(p)
                            else:
                                p2_list.append(p)
        p0 = p0_list[0]
        p1 = p0
        for p in p1_list:
            if cmp_card(p1, p) < 0:
                p1 = p
        p2 = p1
        for p in p2_list:
            if cmp_card(p2, p) < 0:
                p2 = p

        if PRINT_LOG:
            print('-----------------------------')
            print('cards: %s' % _calc_cards)
            print('p0:')
            p0.print_res()
            print('p1:')
            p1.print_res()
            print('p2:')
            p2.print_res()

        d['res'].append(p2)

    winners = [d['res'][0]]
    for i in d['res'][1:]:
        ret = cmp_card(winners[0], i)
        if ret == 0:
            winners.append(i)
        elif ret < 0:
            winners = [i]

    if PRINT_LOG:
        print('Winner: ')
        winners[0].print_res()
    d['winners'] = winners
    return

def cmp_point(p1, p2):
    #print('p1')
    #p1.print_res()
    #print('p2')
    #p2.print_res()
    for i in range(len(p1.cmp_list)):
        if p1.cmp_list[i] == p2.cmp_list[i]:
            continue
        elif p1.cmp_list[i] > p2.cmp_list[i]:
            return 1
        else:
            return -1
    return 0

def cmp_card(p1, p2):
    if p1.all_cards == p2.all_cards:
        return 0
    if not p1.is_flush and not p2.is_flush:
        if p1.points == p2.points:
            return 0

    if p1.is_straight_flush:
        if p2.is_straight_flush:
            return cmp_point(p1, p2)
        else:
            return 1
    elif p1.is_four:
        if p2.is_straight_flush:
            return -1
        if p2.is_four:
            return cmp_point(p1, p2)
        else:
            return 1
    elif p1.is_full_house:
        if p2.is_straight_flush or p2.is_four:
            return -1
        elif p2.is_full_house:
            return cmp_point(p1, p2)
        else:
            return 1
    elif p1.is_flush:
        if p2.is_straight_flush or p2.is_four or p2.is_full_house:
            return -1
        elif p2.is_flush:
            return cmp_point(p1, p2)
        else:
            return 1
    elif p1.is_straight:
        if p2.is_straight_flush or p2.is_four or p2.is_full_house or p2.is_flush:
            return -1
        elif p2.is_straight:
            return cmp_point(p1, p2)
        else:
            return 1
    elif p1.is_three:
        if p2.is_straight_flush or p2.is_four or p2.is_full_house or p2.is_flush or p2.is_straight:
            return -1
        elif p2.is_three:
            return cmp_point(p1, p2)
        else:
            return 1
    elif p1.is_two:
        if p2.is_straight_flush or p2.is_four or p2.is_full_house or p2.is_flush or p2.is_straight or p2.is_three:
            return -1
        elif p2.is_two:
            return cmp_point(p1, p2)
        else:
            return 1
    elif p1.is_one:
        if p2.is_straight_flush or p2.is_four or p2.is_full_house or p2.is_flush or p2.is_straight or p2.is_three or p2.is_two:
            return -1
        elif p2.is_one:
            return cmp_point(p1, p2)
        else:
            return 1
    else:
        if not p2.is_high:
            return -1
        else:
            return cmp_point(p1, p2)

def get_card_str(hand_cards):
    if CARD_POINT_DICT[hand_cards[0]] < CARD_POINT_DICT[hand_cards[1]]:
        _card = [hand_cards[1], hand_cards[0]]
    else:
        _card = hand_cards
    if _card[0][0] == _card[1][0]:
        kstr = '%s%ss' % (_card[0].split('-')[1], _card[1].split('-')[1])
    else:
        kstr = '%s%s' % (_card[0].split('-')[1], _card[1].split('-')[1])
    return kstr

