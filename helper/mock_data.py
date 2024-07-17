from tqdm import tqdm
from faker import Faker
import csv
import os
faker = Faker()
data_folder = os.path.join (os.getcwd(),"data")
if not os.path.exists(data_folder):
    os.mkdir(data_folder)
csv_file = os.path.join (data_folder,"data.csv")
fieldnames = ["name","job","birthdate"]
with open (csv_file,"w") as f:
    writer = csv.DictWriter(f,fieldnames=fieldnames)
    writer.writeheader()
    for _ in range (20):
        _profile = faker.profile()
        writer.writerow({fieldnames[0]:_profile.get(fieldnames[0]),fieldnames[1]:_profile.get(fieldnames[1]),fieldnames[2]:_profile.get(fieldnames[2])})
    