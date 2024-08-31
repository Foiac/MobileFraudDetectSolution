# 
FROM python:3.10.6

# 
WORKDIR /login-authentication

# 
COPY ./requirements.txt /login-authentication/requirements.txt

# 
RUN pip install --no-cache-dir --upgrade -r /login-authentication/requirements.txt

# 
COPY ./app /login-authentication/app

# 
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "80"]