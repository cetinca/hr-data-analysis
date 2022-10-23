import os

import pandas as pd
import requests

# scroll down to the bottom to implement your solution

if __name__ == '__main__':

    if not os.path.exists('../Data'):
        os.mkdir('../Data')

    # Download data if it is unavailable.
    if ('A_office_data.xml' not in os.listdir('../Data') and
            'B_office_data.xml' not in os.listdir('../Data') and
            'hr_data.xml' not in os.listdir('../Data')):
        print('A_office_data loading.')
        url = "https://www.dropbox.com/s/jpeknyzx57c4jb2/A_office_data.xml?dl=1"
        r = requests.get(url, allow_redirects=True)
        open('../Data/A_office_data.xml', 'wb').write(r.content)
        print('Loaded.')

        print('B_office_data loading.')
        url = "https://www.dropbox.com/s/hea0tbhir64u9t5/B_office_data.xml?dl=1"
        r = requests.get(url, allow_redirects=True)
        open('../Data/B_office_data.xml', 'wb').write(r.content)
        print('Loaded.')

        print('hr_data loading.')
        url = "https://www.dropbox.com/s/u6jzqqg1byajy0s/hr_data.xml?dl=1"
        r = requests.get(url, allow_redirects=True)
        open('../Data/hr_data.xml', 'wb').write(r.content)
        print('Loaded.')

        # All data in now loaded to the Data folder.

    # write your code here

    """Load all three datasets. Remember, data is stored in the XML format. You can use the read_xml() function from pandas;
    Explore the data. Check what the axes are, get the shapes of the DataFrames, use df.info() to check whether there are null values, and overview the data types;
    Reindex all three datasets. It is required because some of the employee_office_id column values for offices A and B match. For HR data, use the employee_id column as the index. For A and B offices, use the name of the office and the employee_office_id column as indexes. For example, for office A, employee #125 will be A125. The offices' data index should resemble HR's data index.
    Print three Python lists containing office A, B, and HR data indexes."""

    a_office_df = pd.read_xml("../Data/A_office_data.xml")
    b_office_df = pd.read_xml("../Data/B_office_data.xml")
    hr_df = pd.read_xml("../Data/hr_data.xml")

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    # a_office_df.info()

    a_office_df["employee_id"] = "A" + a_office_df["employee_office_id"].apply(lambda x: str(x))

    b_office_df["employee_id"] = "B" + b_office_df["employee_office_id"].apply(lambda x: str(x))

    a_office_df.set_index(keys="employee_id", drop=True, inplace=True)
    b_office_df.set_index(keys="employee_id", drop=True, inplace=True)
    hr_df.set_index(keys="employee_id", drop=True, inplace=True)

    # r = hr_df.tail(5)

    # test caused infinite loop when I printed one list
    ab_df = pd.concat(objs=[a_office_df, b_office_df])

    # final_df = pd.merge(ab_df, hr_df, left_index=True, right_index=True, how="inner", indicator=True)
    # indicator adds a line show row status for each dataframe

    final_df = pd.merge(ab_df, hr_df, left_index=True, right_index=True, how="inner")

    final_df.drop(["employee_office_id"], inplace=True, axis=1)

    final_df.sort_index(inplace=True)

    # stage 2
    # print(final_df.columns.to_list())

    working_hours = final_df[["Department", "average_monthly_hours"]].sort_values(["average_monthly_hours"],
                                                                                  ascending=False)
    top_departments_by_hours = working_hours["Department"][:10].to_list()

    projects = final_df[["Department", "salary", "number_project"]]
    num_projects = projects[(projects["salary"] == "low") & (projects["Department"] == "IT")].aggregate(
        {"number_project": "sum"})
    e1 = final_df.loc["A4"][["last_evaluation", "satisfaction_level"]].to_list()
    e2 = final_df.loc["B7064"][["last_evaluation", "satisfaction_level"]].to_list()
    e3 = final_df.loc["A3033"][["last_evaluation", "satisfaction_level"]].to_list()


    # stage 3
    # print(top_departments_by_hours)
    # print(num_projects.to_list()[0])
    # print([e1, e2, e3])

    # takes a series and returns a result
    def count_bigger_5(series):
        # print(series)
        return series.where(series > 5).count()


    r = final_df.groupby("left").aggregate(
        {
            "Work_accident": ["mean"],
            "last_evaluation": ["mean", "std"],
            "time_spend_company": ["mean", "median"],
            "number_project": ["median", count_bigger_5]
        }
    ).round(2)

    # for custom aggregate name
    # alternative = final_df.groupby("left").aggregate(
    #     Number_project_median=("number_project", "median"),
    #     Time_spend_company_mean=("time_spend_company", "mean"),
    #     Time_spend_company_median=("time_spend_company", "median"),
    # ).round(2)

    # r = pd.concat([a, b, c, d], axis=1)

    # stage 4
    # print(r.to_dict())

    # stage 5
    # https://towardsdatascience.com/working-with-multi-index-pandas-dataframes-f64d2e2c3e02
    r1 = final_df.pivot_table(
        index="Department",
        columns=["left", "salary"],
        values="average_monthly_hours",
        aggfunc="median"
    ).round(2)

    # last_evaluation     satisfaction_level     last_evaluation

    r1_filtered = r1[(r1[(0, 'high')] < r1[(0, 'medium')]) | (r1[(1, 'low')] < r1[(1, 'high')])]

    print(r1_filtered.to_dict())

    r2 = final_df.pivot_table(
        index="time_spend_company",
        columns="promotion_last_5years",
        values=["last_evaluation", "satisfaction_level"],
        aggfunc=["max", "mean", "min"]
    ).round(2)

    # r2_filtered = r2[r2[('mean', 'last_evaluation', 1)] < r2[('mean', 'last_evaluation', 0)]]
    r2_filtered = r2[r2['mean']['last_evaluation'][1] < r2['mean']['last_evaluation'][0]]

    print(r2_filtered.to_dict())
