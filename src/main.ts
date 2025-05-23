import { config } from 'dotenv';
config();
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
	await NestFactory.createApplicationContext(AppModule);
}
bootstrap();
