FROM node:12.18.1
ENV NODE_ENV=production
# Node image with node_env as production
WORKDIR /app
# Workdir is app
RUN npm i express socket.io kafka-node
# install dependencies
COPY . .
# get the server js file

CMD ["node", "server.js"]
# start the server