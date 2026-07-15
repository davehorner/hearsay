//! Cubelink: a two-player game where each player drives a cube around a large
//! floor with a pan-orbit view and they chat over a retained-UI panel.
//!
//! Networking is host-authoritative (see `net.rs`). Guests send only their
//! movement input; the host owns every cube's position, applies the movement
//! rules, and broadcasts authoritative positions unreliably. Chat goes
//! reliably. All traffic is encrypted with a shared passphrase.

mod net;

use net::{NetCommand, NetEvent, NetLink, PlayerId, spawn_net};
use nightshade_api::prelude::*;

const MOVE_SPEED: f32 = 9.0;
const FLOOR_HALF_EXTENT: f32 = 60.0;
const CAMERA_RADIUS: f32 = 16.0;
const CUBE_HEIGHT: f32 = 0.5;
const LOG_LINES: usize = 6;

const BLUE: [f32; 4] = [0.25, 0.55, 1.0, 1.0];
const ORANGE: [f32; 4] = [1.0, 0.5, 0.2, 1.0];

struct Game {
    net: NetLink,
    own_id: PlayerId,
    // Cube entities indexed by player id.
    cubes: [Entity; 2],
    // The host's authoritative positions and the latest input per player.
    positions: [[f32; 3]; 2],
    inputs: [[f32; 2]; 2],
    input: Entity,
    send_checkbox: Entity,
    send_button: Entity,
    show_log_checkbox: Entity,
    log_labels: Vec<Entity>,
    log: Vec<String>,
    draft: String,
    send_was_checked: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    run!(setup, update)
}

fn setup(world: &mut World) -> Game {
    let net = spawn_net();
    let own_id = net.my_id;

    let floor = spawn_floor(world, FLOOR_HALF_EXTENT);
    set_color(world, floor, [0.14, 0.16, 0.21, 1.0]);
    world.resources.debug_draw.show_grid = true;

    let start_zero = starting_position(0);
    let start_one = starting_position(1);
    let cube_zero = spawn_cube(world, start_zero);
    set_color(world, cube_zero, BLUE);
    let cube_one = spawn_cube(world, start_one);
    set_color(world, cube_one, ORANGE);

    orbit_camera(world, starting_position(own_id), CAMERA_RADIUS);

    let panel = spawn_panel(world, ScreenAnchor::BottomLeft, 380.0, 300.0);
    panel_heading(world, panel, "Chat");
    let show_log_checkbox = panel_checkbox(world, panel, "Show log", true);
    let log_labels: Vec<Entity> = (0..LOG_LINES)
        .map(|_| panel_label(world, panel, ""))
        .collect();
    let input = panel_text_input(world, panel, "Type a message...");
    let send_checkbox = panel_checkbox(world, panel, "Send (toggle to send typed text)", false);
    let send_button = panel_button(world, panel, "Send");

    Game {
        net,
        own_id,
        cubes: [cube_zero, cube_one],
        positions: [
            [start_zero.x, start_zero.y, start_zero.z],
            [start_one.x, start_one.y, start_one.z],
        ],
        inputs: [[0.0, 0.0], [0.0, 0.0]],
        input,
        send_checkbox,
        send_button,
        show_log_checkbox,
        log_labels,
        log: Vec::new(),
        draft: String::new(),
        send_was_checked: false,
    }
}

fn update(world: &mut World, game: &mut Game) {
    apply_network_events(world, game);
    drive(world, game);
    handle_chat(world, game);
    update_log_visibility(world, game);
}

fn apply_network_events(world: &mut World, game: &mut Game) {
    while let Ok(event) = game.net.from_net.try_recv() {
        match event {
            // The host records each guest's latest input to simulate from.
            NetEvent::Input { id, direction } => {
                if game.net.is_host && (id as usize) < game.inputs.len() {
                    game.inputs[id as usize] = direction;
                }
            }
            // A guest renders whatever authoritative position the host sends.
            NetEvent::State { id, position } => {
                if !game.net.is_host && (id as usize) < game.cubes.len() {
                    set_position(
                        world,
                        game.cubes[id as usize],
                        vec3(position[0], position[1], position[2]),
                    );
                }
            }
            NetEvent::Chat { id, text } => {
                push_log(world, game, format!("Player {id}: {text}"));
            }
        }
    }
}

fn drive(world: &mut World, game: &mut Game) {
    let delta = delta_time(world);
    let direction = wasd(world);
    let local_input = [direction.x, direction.z];

    if game.net.is_host {
        // The host applies the movement rules to every cube it owns and
        // broadcasts the result. A guest can only influence its own input.
        game.inputs[game.own_id as usize] = local_input;
        for id in 0..game.cubes.len() {
            let input = game.inputs[id];
            let mut next = game.positions[id];
            next[0] = (next[0] + input[0] * MOVE_SPEED * delta)
                .clamp(-FLOOR_HALF_EXTENT, FLOOR_HALF_EXTENT);
            next[2] = (next[2] + input[1] * MOVE_SPEED * delta)
                .clamp(-FLOOR_HALF_EXTENT, FLOOR_HALF_EXTENT);
            next[1] = CUBE_HEIGHT;
            game.positions[id] = next;
            set_position(world, game.cubes[id], vec3(next[0], next[1], next[2]));
            let _ = game.net.to_net.send(NetCommand::State {
                id: id as PlayerId,
                position: next,
            });
        }
    } else {
        // A guest sends only its input; its cubes are moved by the host's state.
        let _ = game.net.to_net.send(NetCommand::Input(local_input));
    }

    let own_position = position(world, game.cubes[game.own_id as usize]);
    set_orbit_focus(world, own_position);
}

fn handle_chat(world: &mut World, game: &mut Game) {
    if let Some(text) = text_input_changed(world, game.input) {
        game.draft = text;
    }

    let send_checked = checkbox_value(world, game.send_checkbox);
    let checkbox_fired = send_checked && !game.send_was_checked;
    game.send_was_checked = send_checked;
    let button_fired = button_clicked(world, game.send_button);

    if (checkbox_fired || button_fired) && !game.draft.trim().is_empty() {
        let text = game.draft.trim().to_string();
        let _ = game.net.to_net.send(NetCommand::Chat(text.clone()));
        push_log(world, game, format!("You: {text}"));
        game.draft.clear();
    }
}

fn update_log_visibility(world: &mut World, game: &mut Game) {
    let show = checkbox_value(world, game.show_log_checkbox);
    for &label in &game.log_labels {
        set_panel_visible(world, label, show);
    }
}

fn push_log(world: &mut World, game: &mut Game, line: String) {
    game.log.push(line);
    let first_visible = game.log.len().saturating_sub(game.log_labels.len());
    for (index, &label) in game.log_labels.iter().enumerate() {
        let text = game
            .log
            .get(first_visible + index)
            .map(String::as_str)
            .unwrap_or("");
        set_panel_text(world, label, text);
    }
}

fn starting_position(id: PlayerId) -> Vec3 {
    let offset = if id == 0 { -4.0 } else { 4.0 };
    vec3(offset, CUBE_HEIGHT, 0.0)
}
