FROM python:3.6.6

RUN python -m pip install pip --upgrade

COPY setup.py /workflow/
COPY requirements.txt /workflow/
COPY src/ /workflow/src/
COPY test/ /workflow/test/

RUN find . | grep -E "(__pycache__|\.pyc$)" | xargs rm -rf
RUN pip install -U -r workflow/requirements.txt
RUN pip install workflow/.
