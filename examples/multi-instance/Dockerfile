FROM node:20-alpine

WORKDIR /usr/src/app

COPY package*.json ./
COPY tsup.config.ts tsconfig.json ./
COPY src/ ./src/
RUN npm install
RUN npm run build

WORKDIR /usr/src/app/examples/multi-instance
COPY examples/multi-instance/package*.json ./
RUN npm install

COPY examples/multi-instance/server.js ./

CMD ["npm", "start"]
