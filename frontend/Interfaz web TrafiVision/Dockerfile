FROM node:20

WORKDIR /app

COPY package*.json ./
COPY index.html ./
COPY vite.config.ts ./
COPY default_shadcn_theme.css ./
COPY src ./src

RUN npm install

EXPOSE 5173

CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0"]
