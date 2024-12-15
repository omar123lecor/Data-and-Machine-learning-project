from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from seleniumbase import Driver
import random
import s3fs
import time

def main(**kwargs):
    driver = Driver(uc=True, headless=True)
    try:
        #driver.get("https://tracker.gg/valorant/profile/riot/FYOURSK1NS%23diff/matches?season=all&fbclid=IwY2xjawGZqQ5leHRuA2FlbQIxMAABHSSpjw0qSIq4Ryu6txf5TiJKjdrMtGba9HfAEfSxpYFhmM2rujoT_FaG_g_aem_FPf4SmYhNtzZQh2-GF1NPg")
        driver.get("your_target_site")
        liste = []
        i=1
        #macth_per_agents = agent_mactches()
        #all_values = sum(macth_per_agents.values())
        all_data = {
            "Agent" : [],
            "Map" : [],
            "Rank" : [],
            "Ratio K/D" : []
        }
        random_wait = random.uniform(3, 14)
        time.sleep(random_wait)
        try:
            close = WebDriverWait(driver,8).until(
                EC.element_to_be_clickable((By.XPATH,"/html[1]/body[1]/div[1]/div[1]/div[2]/div[3]/div[1]/div[1]/div[1]/div[2]/div[1]/div[1]/div[2]"))
            )
            close.click()
        except Exception:
            pass
        random_wait = random.uniform(3, 14)
        time.sleep(random_wait)
        '''matches = WebDriverWait(driver,40).until(
            EC.element_to_be_clickable((By.XPATH,"/html[1]/body[1]/div[1]/div[1]/div[2]/div[3]/div[1]/main[1]/div[3]/div[1]/div[3]/ul[1]/li[2]/a[1]"))
        )
        matches.click()'''
        while True:
            try:
                load_more = WebDriverWait(driver, 40).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[@class='trn-button']"))
                )
                driver.execute_script("arguments[0].scrollIntoView(true);", load_more)
                random_wait = random.uniform(3, 14)
                time.sleep(random_wait)
                load_more.click()
            except Exception:
                break
        time.sleep(5)
        while True:
            try:
                path = f"/html[1]/body[1]/div[1]/div[1]/div[2]/div[3]/div[1]/main[1]/div[3]/div[2]/div[2]/div[2]/div[1]/div[2]/div[{i}]/div[1]/div[1]/div[1]/div[1]/div[1]/span[1]"
                num = WebDriverWait(driver,20).until(
                    EC.presence_of_element_located((By.XPATH,path))
                )
                i+=1
                num_data_agent_per_zone = int(num.text)
                liste.append(num_data_agent_per_zone)
            except Exception:
                break
        try:
            time.sleep(2)
            c = 1
            for i in liste:
                for j in range(1,i+1):
                    path_agent_name = f"/html[1]/body[1]/div[1]/div[1]/div[2]/div[3]/div[1]/main[1]/div[3]/div[2]/div[2]/div[2]/div[1]/div[2]/div[{c}]/div[2]/div[{j}]/div[1]/div[1]/div[1]/img[1]"
                    agent_name = driver.find_element(By.XPATH,path_agent_name)
                    path_map_name = f"/html[1]/body[1]/div[1]/div[1]/div[2]/div[3]/div[1]/main[1]/div[3]/div[2]/div[2]/div[2]/div[1]/div[2]/div[{c}]/div[2]/div[{j}]/div[1]/div[1]/div[2]/div[2]"
                    map_name = driver.find_element(By.XPATH,path_map_name)
                    path_rank_name = f"/html[1]/body[1]/div[1]/div[1]/div[2]/div[3]/div[1]/main[1]/div[3]/div[2]/div[2]/div[2]/div[1]/div[2]/div[{c}]/div[2]/div[{j}]/div[1]/div[2]/div[1]/img[1]"
                    rank_name = driver.find_element(By.XPATH,path_rank_name)
                    path_ratio_k = f"/html[1]/body[1]/div[1]/div[1]/div[2]/div[3]/div[1]/main[1]/div[3]/div[2]/div[2]/div[2]/div[1]/div[2]/div[{c}]/div[2]/div[{j}]/div[2]/div[2]/div[2]"
                    ratio_k = driver.find_element(By.XPATH,path_ratio_k)
                    all_data["Agent"].append(agent_name.get_attribute("alt"))
                    all_data["Map"].append(map_name.text)
                    all_data["Rank"].append(rank_name.get_attribute("alt"))
                    all_data["Ratio K/D"].append(float(ratio_k.text))
                c +=1
            df = pd.DataFrame(all_data)
            #loading_in_s3(df)
        except Exception as e:
            print(e)
    except Exception as e:
        return
    finally:
        driver.quit()
        return df

def loading_in_s3(**kwargs):
    current_time = time.localtime()
    current_datetime = time.strftime("%Y-%m", current_time)
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='extracting_data')
    aws_access_key_id = 'Your_key_id'
    aws_secret_access_key = 'Your_secret_one'
    fs = s3fs.S3FileSystem(key=aws_access_key_id,secret=aws_secret_access_key)
    s3_path = f"s3://path/vers/valorant{current_datetime}.csv"
    with fs.open(s3_path,'w') as f:
        df.to_csv(f)
    print(f"Data saved to {s3_path} successfully.")

#import subprocess
default_args = {
    'owner':'sabor',
    'retries': 5,
    'retry_delay' : timedelta(minutes=2)
}
with DAG(
    dag_id = 'data_pipline',
    default_args=default_args,
    description = 'sending scrapped data to S3 bucket',
    start_date = datetime(2024,9,12),
    schedule_interval='@monthly',

) as dag:
    task1_pipeline= PythonOperator(
        task_id='extracting_data',
        python_callable=main
    )
    task2_loading_s3 = PythonOperator(
        task_id='loading_data',
        python_callable=loading_in_s3
    )
    task3_runing_ml_modele = TriggerDagRunOperator(
        task_id='once_ready',
        trigger_dag_id='ML_Modele',
        trigger_rule= 'all_success'
    )
    task1_pipeline >> task2_loading_s3 >> task3_runing_ml_modele