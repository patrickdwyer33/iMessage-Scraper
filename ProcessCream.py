import pandas as pd
import sqlite3
import os
import datetime
import time

cur_dir = os.path.dirname(__file__)
parent_dir = os.path.split(cur_dir)[0]
src_data_path = os.path.join(parent_dir, 'Library', 'Messages', 'chat.db')
dst_data_path = os.path.join(os.getcwd(), 'chat.db')

#shutil.copyfile(src_data_path, dst_data_path)
# substitute username with your username
conn = sqlite3.connect(src_data_path)
# connect to the database
cur = conn.cursor()
# get the names of the tables in the database
cur.execute(" select name from sqlite_master where type = 'table' ")
for name in cur.fetchall():
    print(name)

# get the 10 entries of the message table using pandas
messages = pd.read_sql_query("select * from message", conn)
# get the handles to apple-id mapping table
handles = pd.read_sql_query("select * from handle", conn)
# and join to the messages, on handle_id
messages.rename(columns={'ROWID' : 'message_id'}, inplace = True)
handles.rename(columns={'id' : 'phone_number', 'ROWID': 'handle_id'}, inplace = True)
temp = pd.merge(messages[['text', 'handle_id', 'date','is_sent', 'message_id']],  handles[['handle_id', 'phone_number']], on ='handle_id', how='left')

# get the chat to message mapping
chat_message_joins = pd.read_sql_query("select * from chat_message_join", conn)
# and join back to the temp table
cream_data = pd.merge(temp, chat_message_joins[['chat_id', 'message_id']], on = 'message_id', how='left')

cream_data = cream_data[cream_data.chat_id == 976]

cream_data.date = cream_data.date.map(lambda x: x/1000000000)


today = datetime.datetime.today()

start_date = datetime.datetime(2001, 1, 1, 0, 0, 0)
time_difference = abs(today - start_date)
print(time_difference.seconds)
print(cream_data['date'].max())
print(len(cream_data))

def phone_number_cleaner(row):
    sent = row.is_sent
    number = row.phone_number
    if str(sent) == '0':
        return number
    else:
        return '+13128410148'


reaction_list = {'Emphasized', 'Loved', 'Liked',
'Laughed at', 'Disliked', 'Questioned'}


def message_cleaner(row):
    msg = row['text']
    temp_msg = str(msg).split('“')[0].strip()
    for reaction in reaction_list:
        if reaction in temp_msg:
            return reaction
    return 'none'


cream_data.phone_number = cream_data.apply(lambda x: phone_number_cleaner(x), axis = 1)
cream_data['reaction_type'] = cream_data.apply(lambda x: message_cleaner(x), axis = 1)

cream_data.sort_values(by=['date', 'handle_id'], inplace=True)

cream_data.to_csv(os.path.join(os.getcwd(), 'Cream_Data.csv'), index = False, encoding='utf-8')

#reads phone_number to name map from config.txt
number_to_name_map = {}
with open('config.txt', 'r') as f:
    line = f.readline()
    while line:
        line = line.split(':')
        number = line[0]
        name = line[1]
        number_to_name_map[number] = name
        line = f.readline()

creamer_map = {}

for number in phone_number_map.keys():
    name = phone_number_map[number]
    messages_sent = cream_data[cream_data.phone_number == '+'+str(number)]
    counts = []
    index_list = []
    for reaction in reaction_list:
        reaction_data = messages_sent[messages_sent.reaction_type == reaction]
        counts.append(len(reaction_data))
        index_list.append(reaction)
    index_list.append('No Reaction')
    index_list.append('Total')
    normal_data = messages_sent[messages_sent.reaction_type == 'none']
    counts.append(len(normal_data))
    count = len(messages_sent)
    counts.append(count)
    creamer_map[name] = pd.Series(counts, index_list)

love_laugh_data = cream_data[(cream_data.reaction_type == 'Loved') | (cream_data.reaction_type == 'Laughed at')]
text_love_laugh_counts = {}
for idx, row in love_laugh_data.iterrows():
    msg = row['text']
    msg = msg.split('“')[-1]
    msg = msg.split('”')[0]
    msg = msg.strip()
    print(msg)
    type = row['reaction_type']
    text_love_laugh_counts[msg] = text_love_laugh_counts.get(msg, 0) + 1

data = {'text': pd.Series(text_love_laugh_counts.keys(), index=text_love_laugh_counts.keys()), 'love_laugh_count': pd.Series(text_love_laugh_counts.values(), index=text_love_laugh_counts.keys())}
print(data)
df = pd.DataFrame(data=data, index = text_love_laugh_counts.keys())
df.sort_values(by=['love_laugh_count'], inplace=True, ascending=False)
print(df)
#print(creamer_map)

result_data = pd.DataFrame(data=creamer_map, index = ['Emphasized', 'Loved', 'Liked', 'Laughed at', 'Disliked', 'Questioned', 'No Reaction', 'Total'])
result_data.to_csv(os.path.join(os.getcwd(), 'Princeton_Data.csv'), encoding='utf-8')

cur.close()
conn.close()


# convert 2001-01-01 epoch time into a timestamp
# Mac OS X versions after High Sierra
# datetime(message.date/1000000000 + strftime("%s", "2001-01-01") ,"unixepoch","localtime")
# how to use that in the SQL query
# messages = pd.read_sql_query("select *, datetime(message.date/1000000000 + strftime("%s", "2001-01-01") ,"unixepoch","localtime") as date_uct from message", conn)
