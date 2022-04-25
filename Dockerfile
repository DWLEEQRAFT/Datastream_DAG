FROM apache/airflow:2.2.2
#COPY _requires/ /home/airflow/
#RUN pip install /home/airflow/*.whl &&

RUN pip install apache-airflow-providers-docker
RUN pip install google.cloud
RUN pip install pandas
RUN pip install google-cloud-bigquery
RUN pip install db-dtypes

ENV GIT_PYTHON_REFRESH=quiet
ENV TZ=Asia/Seoul%