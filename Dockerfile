FROM node:10.4.0-slim

WORKDIR /usr/app

COPY scripts/versionsSetup.sh .
RUN bash versionsSetup.sh
COPY package.json .
RUN npm install --quiet

COPY . .
