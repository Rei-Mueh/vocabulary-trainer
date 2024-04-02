package de.ksbrwsk.vocabulary;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static java.util.stream.Collectors.toList;

@Component
@Log4j2
@RequiredArgsConstructor
public class DatabaseInitializr {

    private final VocabularyRepository vocabularyRepository;
    private final ApplicationProperties applicationProperties;

    @Scheduled(fixedRateString = "${application.dataRefreshInterval}")
    public void process() throws IOException {
        this.processData();
    }

    private void processData() throws IOException {
        String dataFileUrl = applicationProperties.getDataFileUrl();
        log.info("Vokabeln einlesen und Datenbank initialisieren. Quelle: {}", dataFileUrl);
        Resource resource = readeDataFile(dataFileUrl);
        //Resource resource = new UrlResource(dataFileUrl);
        //Resource resource = new FileSystemResource(dataFileUrl);
        String str = StreamUtils.copyToString(resource.getInputStream(), StandardCharsets.UTF_8);
        String[] tmp = str.split("\n");
        List<String> strings = List.of(tmp);
        List<VocabularyTupel> vcs = strings
                .stream()
                .skip(1) // headers
                .map(String::trim)
                .map(this::splitVcTuple)
                .map(this::createTupel).toList();
        this.vocabularyRepository.reset();
        for (VocabularyTupel tuple : vcs) {
            this.vocabularyRepository.addTupel(tuple);
        }
        log.info("Datenbank erfolgreich initialisiert.");
    }

    private VocabularyTupel createTupel(String[] vTuple) {
        return new VocabularyTupel(Long.parseLong(vTuple[0]), vTuple[1], vTuple[2]);
    }

    private String[] splitVcTuple(String s) {
        s = s.replaceAll("(\r\n|\r)", "");
        return s.split(";");
    }

    private Resource readeDataFile(String dataFileUrl) throws MalformedURLException {
        Resource resource;
        if (dataFileUrl.startsWith("http://") || dataFileUrl.startsWith("https://")) {
            resource = new UrlResource(new URL(dataFileUrl));
        } else {
            resource = new FileSystemResource(dataFileUrl);
        }
        return resource;
    }

}
