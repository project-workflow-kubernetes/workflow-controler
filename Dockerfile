FROM python:3.6.6

# Install docker
RUN apt-get update -qq && \
    apt-get install -qqy apt-transport-https \
        ca-certificates \
        curl \
        lxc \
        gnupg2 \
        software-properties-common \
        iptables && \
    curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg > /tmp/dkey; apt-key add /tmp/dkey && \
    add-apt-repository \
       "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") \
       $(lsb_release -cs) \
       stable" && \
    apt-get update && \
    apt-get -y install docker-ce

RUN sudo service docker start

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
