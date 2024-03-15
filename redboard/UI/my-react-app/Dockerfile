# Step 1: Build the React application
FROM node:alpine as build
RUN apk add --no-cache nodejs npm
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm config set strict-ssl false
RUN npm install
COPY . ./
RUN npm run build

# Step 2: Serve the app using Nginx
FROM nginx:alpine
COPY --from=build /app/build /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]