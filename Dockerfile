FROM python:3.6.6

RUN python -m pip install pip --upgrade

COPY setup.py /workflow/
COPY requirements.txt /workflow/
COPY src/ /workflow/src/

RUN find . | grep -E "(__pycache__|\.pyc$)" | xargs rm -rf
RUN pip install -U -r workflow/requirements.txt
RUN pip install workflow/.

RUN curl -sSL -o /usr/local/bin/argo https://github.com/argoproj/argo/releases/download/v2.2.1/argo-linux-amd64
RUN chmod +x /usr/local/bin/argo

EXPOSE 8000

WORKDIR /workflow/src/workflow
