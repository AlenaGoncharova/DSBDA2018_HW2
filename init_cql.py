# -*- coding: utf-8 -*-
from random import randint

PEOPLE_AMOUNT = 100
OUTPUT_CQL = "fixtures.cql"

INSERT_QUERY_CITIZENS = "INSERT INTO citizens_statistics.citizens (id,passportNumber, ageCategory) VALUES ('{}', '{}', '{}');\n"
INSERT_QUERY_DATA = "INSERT INTO citizens_statistics.data (id,passportNumber, month, data) VALUES ({}, '{}', {}, {});\n"
AGE_CATEGORY = ["old", "young", "middle"]


def generate_people_with_data():
    i = 0
    j = 0

    with open(OUTPUT_CQL, mode='w') as output:
        while i < PEOPLE_AMOUNT:
            # passport format xx xx yyyyyy
            passport_number = str(randint(0, 9))+str(randint(0, 9))+" "+\
                              str(randint(0, 9))+str(randint(0, 9))+" "+\
                              str(randint(0, 9))+str(randint(0, 9))+\
                              str(randint(0, 9))+str(randint(0, 9))+\
                              str(randint(0, 9))+str(randint(0, 9))
            age_category = AGE_CATEGORY[randint(0, 2)]

            # insert query citizen
            citizen_query = INSERT_QUERY_CITIZENS.format(passport_number, passport_number,
                                                         age_category)

            output.write(citizen_query)

            # insert salary
            for _ in range(randint(1, 4)):
                month_number = randint(1, 12)
                salary = randint(1000, 999999)
                salary_query = INSERT_QUERY_DATA.format(j, passport_number,
                                                        month_number, salary)
                j += 1
                output.write(salary_query)

            # insert trips
            for _ in range(randint(1, 4)):
                month_number = randint(1, 12)
                trips = randint(0, 31)
                trips_query = INSERT_QUERY_DATA.format(j, passport_number,
                                                       month_number, trips)
                j += 1
                output.write(trips_query)

            i += 1


if __name__ == "__main__":
    generate_people_with_data()
