package com.manelon.kafkastreams_springboot.utils;

import java.util.ArrayList;
import java.util.List;

import com.github.javafaker.Faker;
import com.manelon.model.User;
import com.manelon.model.UserId;

/**
 * This class generate random users for test proposes
 */
public class TestUsersFactory {

    static Faker faker = new Faker();

    /**
     * Generate a list of TestUsers with positive Id
     * @param number the number of TestUsers to generate
     * @return a list of TestUsers randomly Generated
     */
    public static  List<TestUsers> generateUsers (int number) {
        var list = new ArrayList<TestUsers>();
        
        for (int i=0;i<number;i++){

            int id = i+1;
            var name = faker.name();
            list.add(new TestUsers(
                new UserId(id), 
                new User(id, name.firstName(), name.lastName(),faker.internet().emailAddress(),faker.phoneNumber().cellPhone(), faker.address().fullAddress()))
            );
        }

        return list;
    }
}
