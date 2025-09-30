package com.phatjam98.helpers

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory

class TestLoggingHelpers {
    static ListAppender<ILoggingEvent> getListAppender(Class klass) {
        Logger logger = (Logger) LoggerFactory.getLogger(klass)
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>()
        listAppender.start()
        logger.addAppender(listAppender)

        return listAppender
    }
}
