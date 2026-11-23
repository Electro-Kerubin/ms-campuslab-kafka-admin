package org.campuslab.kafkaadmin.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Metadatos de la documentación OpenAPI (Swagger UI en /swagger-ui.html). */
@Configuration
public class OpenApiConfig {

    private static final String BEARER = "bearerAuth";

    @Bean
    public OpenAPI kafkaAdminOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("CampusLab - Administrador de Kafka")
                        .version("1.0")
                        .description("Crea, consulta, modifica y elimina tópicos y sus configuraciones; inspecciona y administra "
                                + "consumer groups (lag, reinicio de offsets). Requiere un JWT de Azure AD con el rol ADMIN."))
                .components(new Components().addSecuritySchemes(BEARER,
                        new SecurityScheme().type(SecurityScheme.Type.HTTP).scheme("bearer").bearerFormat("JWT")))
                .addSecurityItem(new SecurityRequirement().addList(BEARER));
    }
}
