FROM apache/airflow:2.9.1

COPY requirements.txt /requirements.txt
# Install under the official constraints for this Airflow release so pip
# cannot drift transitive deps away from what the base image was built with
# (unconstrained, it pulled numpy 2.x and broke the image's 1.x-ABI pandas).
RUN pip install --no-cache-dir -r /requirements.txt --constraint \
    "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")').txt"
