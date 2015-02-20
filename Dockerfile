FROM clojure
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY project.clj /usr/src/app/
RUN lein deps
ADD src /usr/src/app/src
RUN ls -lR
RUN mv "$(lein uberjar | sed -n 's/^Created \(.*standalone\.jar\)/\1/p')" app-standalone.jar
ENTRYPOINT ["java", "-jar", "app-standalone.jar"]
