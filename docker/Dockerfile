FROM python:3.12.4-alpine3.20

WORKDIR /att-pipe

COPY . /att-pipe/

RUN pip install --no-cache-dir -r requirements.txt

RUN cp /att-pipe/docker/.docker.pipeline_conf.yml /att-pipe/pipeline_conf.yml

CMD [ "python", "run_pipeline.py", "--log=INFO" ]
