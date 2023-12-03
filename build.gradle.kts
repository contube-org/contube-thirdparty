plugins {
    id("java-library")
    `maven-publish`
    checkstyle
    id("com.github.johnrengelman.shadow") version "7.0.0"
}


var contubeVersion = "1.0-SNAPSHOT"
var pulsarVersion = "3.0.1"
var kafkaVersion = "3.6.0"
var confluentVersion = "7.5.1"
var log4jVersion = "2.14.1"

allprojects {
    buildDir = File("${rootProject.buildDir}/${name}")
    group = "com.zikeyang.contube"
    version = "1.0-SNAPSHOT"
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "checkstyle")
    apply(plugin = "maven-publish")

    repositories {
        mavenLocal()
        mavenCentral()
        maven("https://packages.confluent.io/maven/")
    }

    checkstyle {
        toolVersion = "10.12.4"
        configFile = file("${project.rootDir}/checkstyle/checkstyle.xml")
        isShowViolations = true
    }

    dependencies {
        api("com.zikeyang.contube:contube-common:$contubeVersion")

        testImplementation(platform("org.junit:junit-bom:5.9.1"))
        testImplementation("org.junit.jupiter:junit-jupiter")

        compileOnly("org.projectlombok:lombok:1.18.24")
        annotationProcessor("org.projectlombok:lombok:1.18.24")
    }

    tasks.test {
        useJUnitPlatform()
    }

    tasks.register<Copy>("copyDependencies") {
        from(configurations.runtimeClasspath)
        into("${project.rootDir}/libs")
    }
    tasks.named("jar") {
        dependsOn("copyDependencies")
    }

    publishing {
        publications {
            create<MavenPublication>("ConTube-Pulsar") {
                from(components["java"])
                artifactId = project.name
                version = project.version.toString()
            }
        }
    }
}

project(":contube-pulsar") {
    dependencies {
        implementation("org.apache.pulsar:pulsar-functions-utils:$pulsarVersion")
        implementation("org.apache.pulsar:pulsar-common:$pulsarVersion")
        implementation("org.apache.pulsar:pulsar-client-original:$pulsarVersion")
        implementation("org.apache.bookkeeper:circe-checksum:4.16.2")
    }
}

project(":contube-pulsar-runtime") {
    dependencies {
        implementation(project(":contube-pulsar"))
        implementation("com.zikeyang.contube:contube-runtime:$contubeVersion")
        runtimeOnly("org.apache.logging.log4j:log4j-core:$log4jVersion")
    }
}

project(":contube-kafka") {
    dependencies {
        // TODO: Demo only, remove it later
        implementation("org.apache.pulsar:pulsar-common:$pulsarVersion")
        implementation("org.apache.pulsar:pulsar-client-original:$pulsarVersion")
        implementation(project(":contube-pulsar"))
        // ===
        implementation("org.apache.kafka:connect-runtime:$kafkaVersion")
        implementation("org.apache.kafka:connect-json:$kafkaVersion")
        implementation("org.apache.kafka:connect-api:$kafkaVersion")
        implementation("io.confluent:kafka-connect-avro-converter:$confluentVersion")
    }
}

project(":contube-kafka-runtime") {
    dependencies {
        implementation(project(":contube-kafka"))
        implementation("com.zikeyang.contube:contube-runtime:$contubeVersion")
        runtimeOnly("org.apache.logging.log4j:log4j-core:$log4jVersion")
        runtimeOnly("io.debezium:debezium-connector-mongodb:1.9.7.Final")
    }
}

project(":contube-all") {
    apply(plugin = "com.github.johnrengelman.shadow")

    dependencies {
        runtimeOnly(project(":contube-pulsar-runtime"))
        runtimeOnly(project(":contube-kafka-runtime"))
        runtimeOnly("org.apache.logging.log4j:log4j-core:$log4jVersion")
        runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
    }

    tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
        archiveBaseName.set(project.name)
        archiveClassifier.set("")
        archiveVersion.set(project.version.toString())
    }
}
