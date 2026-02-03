# syntax=docker/dockerfile:1

FROM maven:3.8.6-jdk-11
ENV NODE_ENV=development

WORKDIR /app
# Clone jedis dragonfly fork
RUN git clone -b dragonfly https://github.com/dragonflydb/jedis.git

WORKDIR /app/jedis

# Build the client and tests
RUN mvn test -DskipTests 

# Run selected tests
CMD mvn surefire:test -Dtest="AllKindOfValuesCommandsTest,BitCommandsTest,ControlCommandsTest,ControlCommandsTest,HashesCommandsTest,ListCommandsTest,ScriptingCommandsTest,ScriptingCommandsTest,SetCommandsTest,SetCommandsTest,SetCommandsTest,TransactionCommandsTest,ClientCommandsTest,PublishSubscribeCommandsTest,SortedSetCommandsTest,SortingCommandsTest,StreamsCommandsTest" 

