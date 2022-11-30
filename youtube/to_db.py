import pymysql.cursors
import pandas as pd


host_name = 'your_hostname'
username = 'your_username'
password = 'your_pw'
dbname = 'your_db_name'


def create_table(cs):
    create_table_query = (''' CREATE TABLE if not exists YT_VID
                            (video_id varchar(255) primary key,
                            video_title text not null,
                            published_date date not null,
                            view_count integer not null,
                            like_count integer not null,
                            comment_count integer not null);
                        ''')
    cs.execute(create_table_query)



def check_if_video_exist(cs, vid_id):
    query = ('''SELECT video_id FROM YT_VID WHERE
                video_id=%s; ''')
    cs.execute(query, (vid_id,))
    return cs.fetchone() is not None



def update_row(cs, vid_id, vid_title, published_date, view_count, like_count, cmt_count):
    query = ('''UPDATE YT_VID
                SET video_title=%s,
                published_date=%s,
                view_count=%s,
                like_count=%s,
                comment_count=%s
                WHERE video_id=%s; ''')
    update_vars = (vid_title, published_date, view_count, like_count, cmt_count, vid_id)
    cs.execute(query, update_vars)


def update_db(cs, df, temp_df):
    for _, row in df.iterrows():
        if check_if_video_exist(cs, row['video_id']):
            update_row(cs, row['video_id'], row['video_title'], row['published_date'], row['view_count'], row['like_count'], row['comment_count'])
        else:
            temp_df.loc[len(temp_df.index)] = row
    return temp_df



def insert_table(cs, vid_id, vid_title, published_date, view_count, like_count, cmt_count):

    insert_into_videos = ('''
        INSERT INTO YT_VID (video_id, video_title, published_date,
        view_count, like_count, comment_count)
        VALUES (%s, %s, %s, %s, %s, %s); ''')

    row_to_insert = (vid_id, vid_title, published_date, view_count, like_count, cmt_count)
    cs.execute(insert_into_videos, row_to_insert)


def add_df_to_db(cs, df):
    for _, row in df.iterrows():
        if not check_if_video_exist(cs, row['video_id']):
            insert_table(cs, row['video_id'], row['video_title'],row['published_date'], row['view_count'], row['like_count'], row['comment_count'])


def main(df):
    conn = pymysql.connect(host=host_name, database=dbname, user=username, password=password, 
                        cursorclass=pymysql.cursors.DictCursor)

    with conn:
        with conn.cursor() as cur:
            create_table(cur)

        conn.commit()

        temp_df = pd.DataFrame(columns=df.columns)

        with conn.cursor() as cur:
            new_vid_df = update_db(cur, df, temp_df)
            add_df_to_db(cur, new_vid_df)

        conn.commit()


