package dev.sbs.simplifiedconsole;

import dev.sbs.api.SimplifiedApi;
import dev.sbs.api.client.impl.hypixel.request.HypixelRequest;
import dev.sbs.api.client.impl.mojang.MojangProxy;
import dev.sbs.api.client.impl.sbs.response.MojangProfileResponse;
import dev.sbs.api.data.sql.SqlConfig;
import dev.sbs.api.reflection.Reflection;
import dev.sbs.api.util.NumberUtil;
import dev.sbs.api.util.StringUtil;
import dev.sbs.api.util.SystemUtil;
import dev.sbs.discordapi.DiscordBot;
import dev.sbs.discordapi.DiscordConfig;
import dev.sbs.discordapi.command.DiscordCommand;
import dev.sbs.simplifiedconsole.processor.resource.ResourceCollectionsProcessor;
import dev.sbs.simplifiedconsole.processor.resource.ResourceItemsProcessor;
import dev.sbs.simplifiedconsole.processor.resource.ResourceSkillsProcessor;
import dev.sbs.simplifiedconsole.server.SimplifiedServer;
import discord4j.core.GatewayDiscordClient;
import discord4j.core.object.presence.ClientActivity;
import discord4j.core.object.presence.ClientPresence;
import discord4j.core.shard.MemberRequestFilter;
import discord4j.gateway.intent.Intent;
import discord4j.gateway.intent.IntentSet;
import discord4j.rest.util.AllowedMentions;
import io.netty.channel.ChannelOption;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Level;
import org.jetbrains.annotations.NotNull;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.web.server.Compression;
import org.springframework.util.unit.DataSize;
import reactor.core.publisher.Mono;
import reactor.netty.http.server.HttpServer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Getter
@Log4j2
public class SimplifiedConsole extends DiscordBot {

    private static final HypixelRequest HYPIXEL_RESOURCE_REQUEST = SimplifiedApi.getApiRequest(HypixelRequest.class);

    private SimplifiedConsole(@NotNull DiscordConfig discordConfig) {
        super(discordConfig);

        // TODO: THE MAGIC SAUCE
        SpringApplication springBoot = new SpringApplication(SimplifiedServer.class);
        springBoot.setKeepAlive(true);
        springBoot.setHeadless(true);



        Compression compression = new Compression();
        compression.setEnabled(true);
        compression.setMinResponseSize(DataSize.ofKilobytes(2));
        compression.setMimeTypes(new String[]{"text/html", "text/xml", "text/plain", "text/css", "text/javascript", "application/javascript", "application/json"});



        HttpServer.create()
            .host("::")
            .port(8000)
            .compress(true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .route(routes -> routes.get(
                "/mojang/user/{user}",
                (request, response) -> Mono.fromCallable(() -> {
                        MojangProxy mojangProxy = SimplifiedApi.getMojangProxy();
                        String user = request.param("user"); // Check not null
                        MojangProfileResponse mojangProfileResponse = StringUtil.isUUID(user) ?
                            mojangProxy.getMojangProfile(StringUtil.toUUID(user)) :
                            mojangProxy.getMojangProfile(user);
                        return SimplifiedApi.getGson().toJson(mojangProfileResponse);
                    })
                    .map(Mono::just)
                    .flatMap(json -> response.status(200).sendString(json).then())
            ))
            .bindUntilJavaShutdown(Duration.ofSeconds(30), facade -> System.out.println("Server started"));
    }

    public static void main(String[] args) {
        DiscordConfig discordConfig = DiscordConfig.builder()
            .withToken(SystemUtil.getEnv("DISCORD_TOKEN"))
            .withMainGuildId(652148034448261150L)
            .withDebugChannelId(SystemUtil.getEnv("DEVELOPER_ERROR_LOG_CHANNEL_ID").map(NumberUtil::tryParseLong))
            .withCommands(
                Reflection.getResources()
                    .filterPackage("dev.sbs.simplifiedconsole.command")
                    .getTypesOf(DiscordCommand.class)
            )
            .withDataConfig(SqlConfig.defaultSql())
            .withAllowedMentions(AllowedMentions.suppressEveryone())
            .withDisabledIntents(IntentSet.of(Intent.GUILD_PRESENCES))
            .withClientPresence(ClientPresence.doNotDisturb(ClientActivity.competing("API requests")))
            .withMemberRequestFilter(MemberRequestFilter.all())
            .withLogLevel(Level.INFO)
            .build();

        new SimplifiedConsole(discordConfig);
    }

    @Override
    protected void onGatewayConnected(@NotNull GatewayDiscordClient gatewayDiscordClient) {
        SimplifiedApi.getKeyManager().add(SystemUtil.getEnvPair("HYPIXEL_API_KEY"));
    }

    @Override
    protected void onDatabaseConnected() {
        // Schedule Item Resource Updates
        this.getScheduler().scheduleAsync(() -> {
            try {
                log.info("Processing Items");
                new ResourceItemsProcessor(HYPIXEL_RESOURCE_REQUEST.getItems()).process();
            } catch (Exception exception) {
                log.atError()
                    .withThrowable(exception)
                    .log("An error occurred while processing the resource API.");
            }
        }, 0, 1, TimeUnit.MINUTES);

        // Schedule Skill Resource Updates
        this.getScheduler().scheduleAsync(() -> {
            try {
                log.info("Processing Skills");
                new ResourceSkillsProcessor(HYPIXEL_RESOURCE_REQUEST.getSkills()).process();
            } catch (Exception exception) {
                log.atError()
                    .withThrowable(exception)
                    .log("An error occurred while processing the resource API.");
            }
        }, 0, 1, TimeUnit.MINUTES);

        // Schedule Collection Resource Updates
        this.getScheduler().scheduleAsync(() -> {
            try {
                log.info("Processing Collections");
                new ResourceCollectionsProcessor(HYPIXEL_RESOURCE_REQUEST.getCollections()).process();
            } catch (Exception exception) {
                log.atError()
                    .withThrowable(exception)
                    .log("An error occurred while processing the resource API.");
            }
        }, 0, 1, TimeUnit.MINUTES);
    }

}
