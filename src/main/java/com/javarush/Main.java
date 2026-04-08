package com.javarush;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.javarush.dao.CityDAO;
import com.javarush.dao.CountryDAO;
import com.javarush.domain.City;
import com.javarush.domain.Country;
import com.javarush.domain.CountryLanguage;
import io.lettuce.core.RedisClient;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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

        redisClient = prepareRedisClient(); // пока заглушка
        mapper = new ObjectMapper();
    }

    public static void main(String[] args) {
        Main main = new Main();

        List<City> allCities = main.fetchData(main);

        System.out.println("Cities loaded: " + allCities.size());

        main.shutdown();
    }

    private SessionFactory prepareRelationalDb() {
        Properties properties = new Properties();

        properties.put("hibernate.connection.driver_class", "com.p6spy.engine.spy.P6SpyDriver");
        properties.put("hibernate.connection.url", "jdbc:p6spy:mysql://localhost:3307/world");

        properties.put("hibernate.connection.username", "root");
        properties.put("hibernate.connection.password", "root");

        properties.put("hibernate.dialect", "org.hibernate.dialect.MySQL8Dialect");
        properties.put("hibernate.current_session_context_class", "thread");
        properties.put("hibernate.hbm2ddl.auto", "validate");
        properties.put("hibernate.jdbc.batch_size", "100");

        properties.put("hibernate.show_sql", "true");
        properties.put("hibernate.format_sql", "true");

        return new Configuration()
                .addAnnotatedClass(City.class)
                .addAnnotatedClass(Country.class)
                .addAnnotatedClass(CountryLanguage.class)
                .addProperties(properties)
                .buildSessionFactory();
    }

    private RedisClient prepareRedisClient() {
        // пока заглушка (по заданию)
        return null;
    }

    private void shutdown() {
        if (nonNull(sessionFactory)) {
            sessionFactory.close();
        }
        if (nonNull(redisClient)) {
            redisClient.shutdown();
        }
    }

    private List<City> fetchData(Main main) {
        try (Session session = main.sessionFactory.getCurrentSession()) {

            List<City> allCities = new ArrayList<>();

            session.beginTransaction();

            // 🔥 ВАЖНО: подгружаем все страны заранее
            List<Country> countries = main.countryDAO.getAll();

            int totalCount = main.cityDAO.getTotalCount();
            int step = 500;

            for (int i = 0; i < totalCount; i += step) {
                allCities.addAll(main.cityDAO.getItems(i, step));
            }

            session.getTransaction().commit();

            return allCities;
        }
    }
}