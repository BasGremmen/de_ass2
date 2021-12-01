FROM node:12.18.1
ENV NODE_ENV=production

WORKDIR /app
RUN npm i express socket.io kafka-node
COPY . .

CMD ["node", "server.js"]