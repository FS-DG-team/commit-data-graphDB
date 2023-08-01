FROM jupyter/pyspark-notebook:spark-3.3.0

LABEL mantainer="Dario Bertolino"

# install requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# jupyter notebook extensions
RUN jupyter contrib nbextension install --user
RUN jupyter nbextension enable toc2/main
RUN jupyter nbextension enable codefolding/main
RUN jupyter nbextension enable execute_time/ExecuteTime
RUN jupyter nbextension enable collapsible_headings/main
RUN jupyter nbextension enable autosavetime/main
RUN jupyter nbextension enable latex_envs/latex_envs

EXPOSE 8888
