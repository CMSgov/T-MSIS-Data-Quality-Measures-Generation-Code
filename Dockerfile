# We pin images via digest.
# Image digest that points to Python 3.9.13 as of 7-7-2022
FROM python@sha256:b7e449e11f8c466fbaf021dcc731563cb36a41321420db3cf506ba4d71d33a65

COPY requirements.txt .
RUN python -m pip install --upgrade pip && pip --no-cache-dir install -r requirements.txt && rm requirements.txt
ADD . dq_measures_python

ENV AWS_DEFAULT_REGION us-east-1
WORKDIR dq_measures_python