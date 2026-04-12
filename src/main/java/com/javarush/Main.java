package com.javarush;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.javarush.dao.CityDAO;
import com.javarush.dao.CountryDAO;
import com.javarush.domain.City;
import com.javarush.domain.Country;
import com.javarush.domain.CountryLanguage;
import com.javarush.redis.CityCountry;
import com.javarush.redis.Language;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

public class Main {

    private final SessionFactory sessionFactory;
    private final RedisClient redisClient;
    private final ObjectMapper mapper;

    private final CityDAO cityDAO;
    private final CountryDAO countryDAO;

    public Main() {
        sessionFactory = prepareRelationalDb();
        cityDAO = new CityDAO(sessionFactory);
        countryDAO = new CountryDAO(sessionFactory);

        redisClient = prepareRedisClient();
        mapper = new ObjectMapper();
    }

    public static void main(String[] args) {

        Main main = new Main();

        List<City> allCities = main.fetchData(main);
        List<CityCountry> preparedData = main.transformData(allCities);

        main.pushToRedis(preparedData);

        main.sessionFactory.getCurrentSession().close();

        List<Integer> ids = List.of(3, 2545, 123, 4, 189, 89, 3458, 1189, 10, 102);

        long startRedis = System.currentTimeMillis();
        main.testRedisData(ids);
        long stopRedis = System.currentTimeMillis();

        long startMysql = System.currentTimeMillis();
        main.testMysqlData(ids);
        long stopMysql = System.currentTimeMillis();

        System.out.println("Redis: " + (stopRedis - startRedis) + " ms");
        System.out.println("MySQL: " + (stopMysql - startMysql) + " ms");

        main.shutdown();
    }

    private SessionFactory prepareRelationalDb() {
        Properties properties = new Properties();
        properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
        properties.put(Environment.DRIVER, "com.p6spy.engine.spy.P6SpyDriver");
        properties.put(Environment.URL, "jdbc:p6spy:mysql://localhost:3307/world");
        properties.put(Environment.USER, "root");
        properties.put(Environment.PASS, "root");
        properties.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
        properties.put(Environment.HBM2DDL_AUTO, "validate");

        return new Configuration()
                .addAnnotatedClass(com.javarush.domain.City.class)
                .addAnnotatedClass(com.javarush.domain.Country.class)
                .addAnnotatedClass(com.javarush.domain.CountryLanguage.class)
                .addProperties(properties)
                .buildSessionFactory();
    }

    private RedisClient prepareRedisClient() {
        RedisClient client = RedisClient.create(RedisURI.create("localhost", 6379));
        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            System.out.println("Connected to Redis");
        }
        return client;
    }

    private List<City> fetchData(Main main) {
        try (Session session = main.sessionFactory.getCurrentSession()) {
            List<City> result = new ArrayList<>();
            session.beginTransaction();

            main.countryDAO.getAll(); // оптимизация

            int total = main.cityDAO.getTotalCount();
            int step = 500;

            for (int i = 0; i < total; i += step) {
                result.addAll(main.cityDAO.getItems(i, step));
            }

            session.getTransaction().commit();
            return result;
        }
    }

    private List<CityCountry> transformData(List<City> cities) {
        return cities.stream().map(city -> {
            CityCountry res = new CityCountry();

            res.setId(city.getId());
            res.setName(city.getName());
            res.setPopulation(city.getPopulation());
            res.setDistrict(city.getDistrict());

            Country country = city.getCountry();

            res.setCountryCode(country.getCode());
            res.setAlternativeCountryCode(country.getAlternativeCode());
            res.setCountryName(country.getName());
            res.setContinent(country.getContinent());
            res.setCountryRegion(country.getRegion());
            res.setCountrySurfaceArea(country.getSurfaceArea());
            res.setCountryPopulation(country.getPopulation());

            Set<Language> languages = country.getLanguages().stream().map(cl -> {
                Language lang = new Language();
                lang.setLanguage(cl.getLanguage());
                lang.setIsOfficial(cl.getOfficial());
                lang.setPercentage(cl.getPercentage());
                return lang;
            }).collect(Collectors.toSet());

            res.setLanguages(languages);

            return res;
        }).collect(Collectors.toList());
    }

    private void pushToRedis(List<CityCountry> data) {
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            RedisStringCommands<String, String> sync = connection.sync();

            for (CityCountry item : data) {
                try {
                    sync.set(String.valueOf(item.getId()), mapper.writeValueAsString(item));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void testRedisData(List<Integer> ids) {
        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            RedisStringCommands<String, String> sync = connection.sync();

            for (Integer id : ids) {
                String json = sync.get(String.valueOf(id));
                try {
                    mapper.readValue(json, CityCountry.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void testMysqlData(List<Integer> ids) {
        try (Session session = sessionFactory.getCurrentSession()) {
            session.beginTransaction();

            for (Integer id : ids) {
                City city = cityDAO.getById(id);
                city.getCountry().getLanguages();
            }

            session.getTransaction().commit();
        }
    }

    private void shutdown() {
        if (nonNull(sessionFactory)) {
            sessionFactory.close();
        }
        if (nonNull(redisClient)) {
            redisClient.shutdown();
        }
    }
}