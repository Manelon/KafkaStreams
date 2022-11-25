package com.manelon.kafkastreams_springboot.utils;

import com.manelon.model.User;
import com.manelon.model.UserId;

public record TestUsers(UserId id, User value) {}
