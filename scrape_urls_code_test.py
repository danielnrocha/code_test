import pandas as pd
from urllib import request
from datetime import datetime
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, DatabaseError


def crawl_urls(source_url):
    
    page = request.urlopen(source_url)
    soup = BeautifulSoup(page)
    
    links_list = [link['href'] for link in soup if link.has_attr('href')]

    title = soup.find('div', {'class': 'title'})

    df = pd.DataFrame(links_list, columns=['urls'])
    df = df.assign(page_title=title, extraction_date=datetime.now())
    
    return df


def get_last_100_scrapes(schema, database, session):

    df = pd.read_sql(f"""
                     SELECT 
                        * 
                     FROM 
                         {schema}.{database} 
                     ORDER BY 
                         extraction_date DESC 
                     LIMIT 100
                     """, session.bind)
    
    return df


def send_to_db(df, schema, database, table):

    db_user, db_password, db_host, db_port = 'fake_user', 'fake_password', 'fake_host', 'fake_port'
    params = f'{db_user}:{db_password}@{db_host}:{db_port}/{schema}'

    engine = create_engine("mysql+mysqlconnector://%s" % params, max_identifier_length=128, pool_size=1)
    engine.connect()

    Session = sessionmaker(bind=engine)
    session = Session()

    df.to_sql(table, engine, if_exists='append', chunksize=1000)

    try:
        df.to_sql(schema=schema, name=database, con=engine, chunksize=1000, index=False, if_exists='append')
        session.commit()
    except (OperationalError, DatabaseError):
        session.rollback()

    df_last_100 = get_last_100_scrapes(schema, database, session)
    
    engine.dispose()
    session.close()

    return df_last_100



if __name__ == '__main__':
    
    source_url = "http://www.google.com"
    schema, database, table = 'urls_schema', 'urls_database', 'urls'
    df = crawl_urls(source_url)
    df_last_100 = send_to_db(df, schema, database, table)
    
    print(df)
    