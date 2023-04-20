#coding:utf-8

import random
from operator import itemgetter
from collections import defaultdict

DIAMONDS = ['diamond-A', 'diamond-2', 'diamond-3', 'diamond-4', 'diamond-5', 'diamond-6',
         'diamond-7', 'diamond-8', 'diamond-9', 'diamond-10', 'diamond-J', 'diamond-Q', 'diamond-K',]
HEARTS = ['heart-A', 'heart-2', 'heart-3', 'heart-4', 'heart-5', 'heart-6',
         'heart-7', 'heart-8', 'heart-9', 'heart-10', 'heart-J', 'heart-Q', 'heart-K',]
SPADES = ['spade-A', 'spade-2', 'spade-3', 'spade-4', 'spade-5', 'spade-6',
         'spade-7', 'spade-8', 'spade-9', 'spade-10', 'spade-J', 'spade-Q', 'spade-K', ]
CLUBS = ['club-A', 'club-2', 'club-3', 'club-4', 'club-5', 'club-6',
         'club-7', 'club-8', 'club-9', 'club-10', 'club-J', 'club-Q', 'club-K']

# 初始化为牌对应的点数， 2、3、4、5、6、7、8、9、10、11、12、13、14， A对应14
CARD_POINT_DICT = {

}

ALL_52_CARDS = DIAMONDS + HEARTS + SPADES + CLUBS

# 是否打印log，0: 不打印， 1: 全部打印
PRINT_LOG = 0


class PlayerCardOption():
    def __init__(self, hand_cards=[], hole_cards=[], all_cards=[]):
        self.hand_cards = hand_cards
        self.hole_cards = hole_cards
        self.all_cards = all_cards
        self.is_straight_flush = False
        self.is_flush = False
        self.is_straight = False
        self.is_four = False
        self.is_full_house = False
        self.is_three = False
        self.is_two = False
        self.is_one = False
        self.is_high = False
        self.cmp_list = []

        self.all_cards.sort()
        self.points = []
        for i in self.all_cards:
            self.points.append(CARD_POINT_DICT[i])
        self.points.sort()

    def calc_cards(self):
        self.is_flush = self.__is_flush()
        self.is_straight = self.__is_straight()
        if self.is_flush and self.is_straight:
            self.is_straight_flush = True
            self.cmp_list.append(self.points[4])
            return
        self.is_four = self.__is_four()
        if self.is_four:
            return
        self.is_full_house = self.__is_full_house()
        if self.is_full_house:
            return
        if self.is_flush:
            for i in self.points:
                self.cmp_list.append(i)
            self.cmp_list.reverse()
            return
        if self.is_straight:
            self.cmp_list.append(self.points[4])
            return
        self.is_three = self.__is_three()
        if self.is_three:
            return
        self.is_two = self.__is_two()
        if self.is_two:
            return
        self.is_one = self.__is_one()
        if self.is_one:
            return

        self.is_high = True
        for i in self.points:
            self.cmp_list.append(i)
        self.cmp_list.reverse()
        return

    def __is_flush(self):
        if self.all_cards[0][0] == self.all_cards[1][0] and \
                self.all_cards[0][0] == self.all_cards[2][0] and \
                self.all_cards[0][0] == self.all_cards[3][0] and \
                self.all_cards[0][0] == self.all_cards[4][0]:
            return True
        else:
            return False

    def __is_straight(self):
        if self.points[0] + 1 == self.points[1] and \
                self.points[1] + 1 == self.points[2] and \
                self.points[2] + 1 == self.points[3] and \
                self.points[3] + 1 == self.points[4]:
            return True
        return False

    def __is_four(self):
        if self.points[0] == self.points[1] and self.points[1] == self.points[2] and self.points[2] == self.points[3]:
            self.cmp_list.append(self.points[0])
            self.cmp_list.append(self.points[4])
            return True
        if self.points[1] == self.points[2] and self.points[2] == self.points[3] and self.points[3] == self.points[4]:
            self.cmp_list.append(self.points[1])
            self.cmp_list.append(self.points[0])
            return True
        return False

    def __is_full_house(self):
        if self.points[0] == self.points[1] and self.points[1] == self.points[2] and self.points[3] == self.points[4]:
            self.cmp_list.append(self.points[0])
            self.cmp_list.append(self.points[3])
            return True
        if self.points[0] == self.points[1] and self.points[2] == self.points[3] and self.points[3] == self.points[4]:
            self.cmp_list.append(self.points[2])
            self.cmp_list.append(self.points[0])
            return True
        return False

    def __is_three(self):
        if self.points[0] == self.points[1] and self.points[1] == self.points[2]:
            self.cmp_list.append(self.points[0])
            self.cmp_list.append(self.points[4])
            self.cmp_list.append(self.points[3])
            return True
        if self.points[1] == self.points[2] and self.points[2] == self.points[3]:
            self.cmp_list.append(self.points[1])
            self.cmp_list.append(self.points[4])
            self.cmp_list.append(self.points[0])
            return True
        if self.points[2] == self.points[3] and self.points[3] == self.points[4]:
            self.cmp_list.append(self.points[2])
            self.cmp_list.append(self.points[1])
            self.cmp_list.append(self.points[0])
            return True
        return False

    def __is_two(self):
        if self.points[0] == self.points[1] and self.points[2] == self.points[3]:
            self.cmp_list.append(self.points[2])
            self.cmp_list.append(self.points[0])
            self.cmp_list.append(self.points[4])
            return True
        if self.points[0] == self.points[1] and self.points[3] == self.points[4]:
            self.cmp_list.append(self.points[3])
            self.cmp_list.append(self.points[1])
            self.cmp_list.append(self.points[2])
            return True
        if self.points[1] == self.points[2] and self.points[3] == self.points[4]:
            self.cmp_list.append(self.points[3])
            self.cmp_list.append(self.points[1])
            self.cmp_list.append(self.points[0])
            return True
        return False

    def __is_one(self):
        if self.points[0] == self.points[1]:
            self.cmp_list.append(self.points[0])
            self.cmp_list.append(self.points[4])
            self.cmp_list.append(self.points[3])
            self.cmp_list.append(self.points[2])
            return True
        if self.points[1] == self.points[2]:
            self.cmp_list.append(self.points[1])
            self.cmp_list.append(self.points[4])
            self.cmp_list.append(self.points[3])
            self.cmp_list.append(self.points[0])
            return True
        if self.points[2] == self.points[3]:
            self.cmp_list.append(self.points[2])
            self.cmp_list.append(self.points[4])
            self.cmp_list.append(self.points[1])
            self.cmp_list.append(self.points[0])
            return True
        if self.points[3] == self.points[4]:
            self.cmp_list.append(self.points[3])
            self.cmp_list.append(self.points[2])
            self.cmp_list.append(self.points[1])
            self.cmp_list.append(self.points[0])
            return True
        return False

    def print_res(self):
        if self.is_straight_flush:
            print('is_straight_flush: %s' % self.points)
        elif self.is_four:
            print('is_four: %s' % self.points)
        elif self.is_full_house:
            print('is_full_house: %s' % self.points)
        elif self.is_flush:
            print('is_flush: %s' % self.points)
        elif self.is_straight:
            print('is_straight: %s' % self.points)
        elif self.is_three:
            print('is_three: %s' % self.points)
        elif self.is_two:
            print('is_two: %s' % self.points)
        elif self.is_one:
            print('is_one: %s' % self.points)
        elif self.is_high:
            print('is_high: %s' % self.points)
        else:
            print('error!!!')

