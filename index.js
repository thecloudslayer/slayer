const { gql } = require('apollo-server');
const { ApolloServer } = require('apollo-server');
const Aerospike = require('aerospike');


const typeDefs = gql`
    type User {
        id: ID!
        name: String!
        age: Int!
        car: String!
    }
    input UserInput {
        id: ID!
        name: String!
        age: Int!
        car: String!
    }

    type Mutation {
        addUser(input: UserInput!): User
    }

    type Query {
        user(id: ID!): User
    }

    type Query {
        allUsers: [User!]!
    }
`;


const config = {hosts: '127.0.0.1:3000', useAlternateAccessAddress: true,};





const resolvers = {
    Query: {
        user: async (_, { id }) => {
            try {
                let client = await Aerospike.connect(config);
                const key = new Aerospike.Key('test', 'users', id);
                const record = await client.get(key);
                return { id, ...record.bins };


               


            } catch (error) {
                console.error(error);
                throw new Error('Failed to fetch user');
            }
        },
        allUsers: async () => {
            try {
                let client = await Aerospike.connect(config);
                const users = [];
                const scan = client.scan('test', 'users'); 
                const stream = scan.foreach();

                return new Promise((resolve, reject) => {
                    stream.on('data', record => {
                        users.push({ id: record.key.digest.toString('base64'), ...record.bins });
                    });
                    stream.on('error', error => {
                        reject(error);
                    });
                    stream.on('end', () => {
                        resolve(users);
                    });
                });
            } catch (error) {
                console.error(error);
                throw new Error('Failed to fetch all users');
            }
        }
    },
    Mutation: {
        addUser: async (_, { input }) => {
            try {
                let client = await Aerospike.connect(config);
                const key = new Aerospike.Key('test', 'users', input.id);
                await client.put(key, input);
                return input;
            } catch (error) {
                console.error(error);
                throw new Error('Failed to add user');
            }
        }
    }
};



const server = new ApolloServer({ typeDefs, resolvers });
server.listen().then(({ url }) => {
    console.log(`🚀 Server ready at ${url}`);
});

