# Step 1: Build the React application
FROM node:alpine as build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm config set strict-ssl false
RUN npm install @emotion/styled --save
RUN npm install
RUN npm install @babel/plugin-proposal-private-property-in-object --save-dev
COPY . ./
RUN npm run build

# Step 2: Serve the app using Nginx
FROM nginx:stable-alpine
COPY --from=build /app/build /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 3005
CMD ["nginx", "-g", "daemon off;"]