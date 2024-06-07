# Pyspark
FROM jupyter/pyspark-notebook:spark-3.5.0
LABEL maintainer="Berg Lloyd-Haig <berg@uq.edu.au>"
#Adaptado de  https://github.com/berglh/jupyterlab/blob/main/Dockerfile

USER root
# De acodo com https://github.com/jupyter/docker-stacks/blob/main/images/pyspark-notebook/Dockerfile
#a versão do pacote isntalado openjdk-17-jre-headless
# A linha abaixo remove o pacote openjdk-17-jre-headless e instala a versão mais nova openjdk-21-jre-headless
# A versão mais nova pode ser obitda em https://packages.ubuntu.com/search?suite=default&section=all&arch=any&keywords=openjdk&searchon=names
# You can revert to JRE8 to avoid errors on Pyspark load (Reflection Errors)
RUN apt-get update && apt-get purge -y openjdk-17-jre-headless && apt-get install -y openjdk-21-jre-headless curl && apt-get -y autoremove && apt-get clean
# othwerise, just cleanup apt in case
RUN apt-get -y autoremove && apt-get clean

# add logging config, removes excessive warnings messages from notebooks
#ADD files/log4j.properties /usr/local/spark/conf

# configure for jovyan
RUN printf 'jovyan    ALL=(ALL:ALL) ALL' >> /etc/sudoers.d/jovyan

# set default jovyan password as jupyter:  echo 'jupyter' | openssl passwd -5 -stdin
RUN printf 'jovyan:$5$evQjlKuZj$VfwL7JB8vTC7MFW6Zvo7OzPk3aXSnRiPST77ogUOnk/' | chpasswd --encrypted

# change back to jovyan user
USER ${NB_UID}

# install and upgrade jupyterlab extensions
RUN pip uninstall pyspark
RUN pip install --upgrade 'jupyterlab>=3' 'jupyterlab_server' 'pyspark==3.5.0' 'plotly'

# instal spark monitor for spark session UI integreation to notebook
RUN pip install jupyterlab-sparkmonitor plotly
RUN ipython profile create --ipython-dir=/home/${NB_USER}/.ipython
RUN echo "c.InteractiveShellApp.extensions.append('sparkmonitor.kernelextension')" >> /home/${NB_USER}/.ipython/profile_default/ipython_config.py
RUN jupyter nbextension install --py --symlink --sys-prefix sparkmonitor
RUN jupyter nbextension enable --py --sys-prefix sparkmonitor

# plotly needs a lab extension also
RUN jupyter labextension install jupyterlab-plotly

# this rebuiltd jupyter lab to register extensions correctly
RUN jupyter lab build
