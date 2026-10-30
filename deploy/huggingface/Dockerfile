FROM node:20-slim
WORKDIR /app
COPY --chown=node:node . /app
ENV PORT=7860
ENV DATA_DIR=/tmp/data
USER node
EXPOSE 7860
CMD ["node", "server.js"]
