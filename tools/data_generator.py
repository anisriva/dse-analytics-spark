import sys
from random import randint, choice, randrange
from tqdm import tqdm
from datetime import timedelta, datetime
import subprocess

def generate_letter():
    lower = chr(randint(ord('a'), ord('z')))
    upper = chr(randint(ord('A'), ord('Z')))
    return choice([lower, upper])


def generate_word(size):
    word = ''
    for n in range(size):
        word += generate_letter()
    return word


def generate_date(start='2000-01-01', end='2001-01-01'):
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


def generate_emptable(n):
    # empid, deptid(1-10), empname, salary
    empid=0
    tmp_sql=''
    for _ in tqdm(range(n)):
        empid+=1
        depid=randint(1, 10)
        empname=generate_word(7) + ' ' + generate_word(10)
        salary=randint(10000, 1000000)

        val='(' + "'" + str(empid) + "', " + "'" + str(depid) + "', " \
            + "'" + empname + "', " + "'" + str(salary) + "'" + "), "
        val=val[:-2]

        tmp_sql+= f"\nINSERT INTO \"nitro\".\"emp\"(empid, depid, empname, salary) \nVALUES {val};"

    output = f"""{tmp_sql}\n"""

    return output

def generate_deptable(n):
    # depid(1-10), empid, depname, locationid(1-50)
    empid=0
    tmp_sql=''
    for _ in tqdm(range(n)):
        depid=randint(1, 10)
        empid+=1     
        depmgr=generate_word(7)
        locid=randint(1, 50)

        val='(' + "'" + str(depid) + "', " + "'" + str(empid) + "', " \
            + "'" + depmgr + "', " + "'" + str(locid) + "'" + "), "
        val=val[:-2]

        tmp_sql+= f"\nINSERT INTO \"nitro\".\"dept\"(depid, empid, depname, locationid) \nVALUES {val};"

    output = f"""{tmp_sql}\n"""

    return output

def generate_inserts():
    return "INSERT INTO inventory.customers(id,first_name,last_name,email) VALUES ({},'{}','{}','{}');".format(randint(1,int(9999999)), generate_word(10), generate_word(15), generate_word(20), generate_word(20))

emp = generate_emptable(int(sys.argv[1]))
dep = generate_deptable(int(sys.argv[1]))

f = open('emptable.txt', 'w')
f.write(emp)
f.close()

f = open('deptable.txt', 'w')
f.write(dep)
f.close()

subprocess.run(['cqlsh', '10.100.200.161', '-f', 'emptable.txt'], stdout=subprocess.PIPE)
subprocess.run(['cqlsh', '10.100.200.161', '-f', 'deptable.txt'], stdout=subprocess.PIPE)
