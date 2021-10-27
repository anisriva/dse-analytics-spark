import sys 
from random import randint, choice, randrange, uniform
from datetime import timedelta, datetime

class Generate:

    @classmethod
    def generate_int(cls, start_val=-(2**31 -1 ), end_val=(2**31 -1 )):
        return randint(start_val, end_val)

    @classmethod
    def generate_bigint(cls, start_val=-(2**63 -1 ), end_val=(2**63 -1 )):
        return randint(start_val, end_val)

    @classmethod
    def generate_float(cls, start_val=-(2**31 -1 ), end_val=(2**31 -1 )):
        return uniform(start_val, end_val)

    @classmethod
    def generate_char(cls):
        lower = chr(randint(ord('a'), ord('z')))
        upper = chr(randint(ord('A'), ord('Z')))
        return choice([lower, upper])

    @classmethod
    def generate_string(cls, size=5):
        word = ''
        for n in range(size):
            word += cls.generate_char()
        return word

    @classmethod
    def generate_date(cls, start='2000-01-01', end='2050-01-01'):
        """
        This function will return a random datetime between two datetime 
        objects.
        """
        d1 = datetime.strptime(start, '%Y-%m-%d')
        d2 = datetime.strptime(end, '%Y-%m-%d')
        delta = d2 - d1
        int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
        random_second = randrange(int_delta)
        return d1 + timedelta(seconds=random_second)