FROM node:10.4.0-slim
WORKDIR /usr/app
COPY package.json .
RUN npm install --quiet
COPY . .
